# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import zookeeper
from zope import interface
from twisted.application import service
from twisted.python import failure
from twisted.internet import defer
from txzookeeper import client

from piped import resource, event, log, exceptions

from piped_zookeeper import log_stream


class DisconnectException(exceptions.PipedError):
    pass


class ZookeeperClientProvider(object, service.MultiService):
    """ Zookeeper support for Piped services.

    Configuration example:

    .. code-block:: yaml

        zookeeper:
            install_log_stream: true # default. handles the zookeeper log stream with piped.log
            clients:
                my_client:
                    servers: localhost:2181
                    events:
                        starting: my_processor

    Available keys for events are: 'starting', 'stopping', 'connected', 'reconnecting', 'reconnected', 'expired'
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._client_by_name = dict()

    def configure(self, runtime_environment):
        self.setName('zookeeper')
        self.setServiceParent(runtime_environment.application)
        self.runtime_environment = runtime_environment

        install_log_stream = runtime_environment.get_configuration_value('zookeeper.install_log_stream', True)
        if install_log_stream:
            log_stream.install()

        self.clients = runtime_environment.get_configuration_value('zookeeper.clients', dict())
        resource_manager = runtime_environment.resource_manager

        for client_name, client_configuration in self.clients.items():
            resource_manager.register('zookeeper.client.%s' % client_name, provider=self)
            # create the client if we have any event processors
            if client_configuration.get('events', None):
                self._get_or_create_client(client_name)

    def add_consumer(self, resource_dependency):
        client_name = resource_dependency.provider.rsplit('.', 1)[-1]
        client = self._get_or_create_client(client_name)

        client.on_connected += resource_dependency.on_resource_ready
        client.on_disconnected += resource_dependency.on_resource_lost

        if client.connected:
            resource_dependency.on_resource_ready(client)

    def _get_or_create_client(self, client_name):
        if client_name not in self._client_by_name:
            client_config = self.clients[client_name]
            txclient = PipedZookeeperClient(**client_config)
            txclient.configure(self.runtime_environment)
            txclient.setServiceParent(self)
            self._client_by_name[client_name] = txclient

        return self._client_by_name[client_name]


class PipedZookeeperClient(client.ZookeeperClient, service.Service):
    possible_events = ('starting', 'stopping', 'connected', 'reconnecting', 'reconnected', 'expired')
    connecting = None
    connected = False
    
    def __init__(self, events = None, *a, **kw):
        super(PipedZookeeperClient, self).__init__(*a, **kw)
        self.events = events or dict()

        self.on_connected = event.Event()
        self.on_connected += lambda _: setattr(self, 'connected', True)
        self.on_disconnected = event.Event()
        self.on_disconnected += lambda _: setattr(self, 'connected', False)

        self.set_session_callback(self._watch_connection)
        self._cache = dict()
        self.on_disconnected += lambda _: self._cache.clear()
        self._pending = dict()

        self.cached_get_children = self._cached(self.get_children_and_watch)
        self.cached_get = self._cached(self.get_and_watch)
        self.cached_exists = self._cached(self.exists_and_watch)

    def configure(self, runtime_environment):
        for key, value in self.events.items():
            if key not in self.possible_events:
                e_msg = 'Invalid event: {0}.'.format(key)
                detail = 'Use one of the possible events: {0}'.format(self.possible_events)
                raise exceptions.ConfigurationError(e_msg, detail)

            self.events[key] = dict(provider=value) if isinstance(value, basestring) else value
        
        self.dependencies = runtime_environment.create_dependency_map(self, **self.events)

    def _started(self):
        self.connecting = None
        self.on_connected(self)
        self._on_event('connected')

    @defer.inlineCallbacks
    def _on_event(self, event_name):
        baton = dict(event=event_name, client=self)

        try:
            processor = yield self.dependencies.wait_for_resource(event_name)
            yield processor(baton)
        except KeyError as ae:
            # we have no processor for this event
            pass

    @defer.inlineCallbacks
    def _watch_connection(self, client, event):
        # see client.STATE_NAME_MAPPING for possible values for event.state_name
        if event.state_name == 'connected':
            self.on_connected(self)
            self._on_event('reconnected')
        elif event.state_name == 'connecting':
            # TODO: if we're in "connecting" for too long, give up and give us a new connection (old session might be bad
            #       because the server rollbacked -- see https://issues.apache.org/jira/browse/ZOOKEEPER-832)
            self.on_disconnected(failure.Failure(DisconnectException(event.state_name)))
            self._on_event('reconnecting')
        elif event.state_name == 'expired':
            self.on_disconnected(failure.Failure(DisconnectException(event.state_name)))
            self._on_event(event.state_name)
            # force a full reconnect in order to ensure we get a new session
            yield self.stopService()
            yield self.startService()
        else:
            log.warn('Unhandled event: {0}'.format(event))

    def startService(self):
        if not self.running:
            service.Service.startService(self)
            self._on_event('starting')
            if self.connecting:
                log.warn('Started connecting before previous connect finished.')
                return
            # TODO: what if we have to stop/start during connecting?
            # TODO: handle connection timeouts
            # set a really high timeout (1 year) because we want txzookeeper to keep
            # trying to connect for a considerable amount of time.
            self.connecting = self.connect(timeout=60*60*24*365)
            return self.connecting.addCallback(lambda _: self._started())

    def stopService(self):
        if self.running:
            service.Service.stopService(self)
            self.on_disconnected(failure.Failure(DisconnectException('stopping service')))
            self._on_event('stopping')
            return self.close()

    def _cached(self, func):
        def wrapper(*a, **kw):
            # determine cache key
            kwargs = kw.items()
            kwargs.sort(key=lambda (k,v): k)
            cache_tuple = (func.func_name,) + a + tuple(value for key, value in kwargs)

            # see if we have the cached results
            if cache_tuple in self._cache:
                return defer.succeed(self._cache[cache_tuple])

            # if we don't, see if we're already waiting for the results
            if cache_tuple in self._pending:
                d = defer.Deferred()
                self._pending[cache_tuple] += d.callback
                return d

            # we're the first one in our process attempting to access this cached result,
            # so we get the honors of setting it up
            self._pending[cache_tuple] = event.Event()
            
            d, watcher = func(*a, **kw)

            def _watch_fired(event):
                # TODO: Determine whether it is possible that the watch fires before the
                # result has been cached, in which case we need to clear self._pending here.
                self._cache.pop(cache_tuple, None)
                return event

            watcher.addBoth(_watch_fired)

            #   return result when available, but remember to inform any other pending waiters.
            def _cache(result):
                if not isinstance(result, failure.Failure):
                    self._cache[cache_tuple] = result

                pending = self._pending.pop(cache_tuple)
                pending(result)
                return result

            d.addBoth(_cache)
            return d

        return wrapper

    @defer.inlineCallbacks
    def delete_recursive(self, path):
        """ Tries to recursively delete nodes under *path*.

        If another process is concurrently creating nodes within the sub-tree, this may
        take a little while to return, as it is *very* persistent about not returning before
        the tree has been deleted, even if it takes multiple tries.
        """
        while True:
            try:
                yield self.delete(path)
            except zookeeper.NoNodeException as nne:
                break
            except zookeeper.NotEmptyException as nee:
                try:
                    children = yield self.get_children(path)
                    ds = []
                    for child in children:
                        ds.append(self.delete_recursive(path + '/' + child))

                    yield defer.DeferredList(ds)

                except zookeeper.NoNodeException as nne:
                    continue
