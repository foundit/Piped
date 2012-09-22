# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import logging
import itertools

import zookeeper
from zope import interface
from twisted.application import service
from twisted.python import failure
from twisted.internet import defer
from txzookeeper import client

from piped import resource, event, exceptions, util

from piped_zookeeper import log_stream


logger = logging.getLogger(__name__)


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
                    reuse_session: true # if false, never re-uses a session if it expires.
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


class ZookeeperClient(client.ZookeeperClient):

    def _check_result(self, result_code, deferred, extra_codes=(), path=None):
        """ Overridden to provide tracebacks on exceptions """
        d = defer.Deferred()
        result = super(ZookeeperClient, self)._check_result(result_code, deferred, extra_codes=(), path=None)
        if d.called:
            maybe_error = d.result
            if isinstance(maybe_error, Exception):
                maybe_error = failure.Failure(maybe_error)
            if isinstance(maybe_error, failure.Failure):
                try:
                    raise maybe_error.type, maybe_error.value
                except Exception as e:
                    # add an empty errback since we're handling the failure by forwarding the exception
                    d.addErrback(lambda _: None)
                    deferred.errback()
                    return result
        d.chainDeferred(deferred)
        return result


class PipedZookeeperClient(object, service.Service):
    possible_events = ('starting', 'stopping', 'connected', 'reconnecting', 'reconnected', 'expired')
    connected = False
    _current_client = None
    _currently_connecting = None
    _currently_reconnecting = None
    
    def __init__(self, servers=None, connect_timeout=86400, reconnect_timeout=30, session_timeout=None, reuse_session=True, events=None):
        self.servers = self._parse_servers(servers)
        self.connect_timeout = connect_timeout
        self.reconnect_timeout = reconnect_timeout
        self.session_timeout = self._session_timeout = session_timeout
        self.reuse_session = reuse_session
        self.events = events or dict()

        self.on_connected = event.Event()
        self.on_connected += lambda _: setattr(self, 'connected', True)
        self.on_disconnected = event.Event()
        self.on_disconnected += lambda _: setattr(self, 'connected', False)

        self._cache = dict()
        self.on_disconnected += lambda _: self._cache.clear()
        self._pending = dict()

        self.connecting_currently = util.create_deferred_state_watcher(self, '_currently_connecting')
        self.reconnecting_currently = util.create_deferred_state_watcher(self, '_currently_reconnecting')

    def _parse_servers(self, servers):
        if isinstance(servers, (list, tuple)):
            return list(servers)

        return servers.split(',')

    def configure(self, runtime_environment):
        for key, value in self.events.items():
            if key not in self.possible_events:
                e_msg = 'Invalid event: {0}.'.format(key)
                detail = 'Use one of the possible events: {0}'.format(self.possible_events)
                raise exceptions.ConfigurationError(e_msg, detail)

            self.events[key] = dict(provider=value) if isinstance(value, basestring) else value
        
        self.dependencies = runtime_environment.create_dependency_map(self, **self.events)

    @defer.inlineCallbacks
    def _start_connecting(self):
        try:
            while self.running:
                try:
                    for server_list_length in range(len(self.servers), 0, -1):
                        if not self.running:
                            break

                        for server_list in itertools.combinations(self.servers, server_list_length):
                            self.on_disconnected(failure.Failure(DisconnectException('connecting')))

                            servers = ','.join(list(server_list))
                            logger.info('Trying to create and connect a ZooKeeper client with the following servers: [{0}]'.format(servers))
                            self._current_client = current_client = self._create_client(servers)

                            try:
                                connected_client = yield self.connecting_currently(self._current_client.connect(timeout=self.connect_timeout))
                                if connected_client == self._current_client:
                                    yield self.connecting_currently(self._started(connected_client))
                            except client.ConnectionTimeoutException as cte:
                                logger.error('Connection timeout reached while trying to connect to ZooKeeper [{0}]: [{1!r}]'.format(server_list, cte))
                                # the server list might be good, so we retry from the beginning with our configured server list.
                                break

                            except zookeeper.ZooKeeperException as e:
                                logger.error('Cannot connect to ZooKeeper [{0}]: [{1!r}]'.format(server_list, e))

                                yield self.connecting_currently(util.wait(0))

                                if not current_client.handle:
                                    # we were unable to actually get a handle, so one of the servers in the server list might be bad.
                                    logger.warn('One of the servers in the server list [{0}] might be invalid somehow.'.format(server_list))
                                    continue

                                defer.maybeDeferred(current_client.close).addBoth(lambda _: None)
                                self._current_client = None
                                continue

                            current_state = 0
                            try:
                                current_state = current_client.state
                            except zookeeper.ZooKeeperException as zke:
                                pass

                            if not current_state == zookeeper.CONNECTED_STATE:
                                logger.info('ZooKeeper client was unable to reach the connected state. Was in [{0}]'.format(client.STATE_NAME_MAPPING.get(current_state, 'unknwown')))
                                current_client.close()
                                if self._current_client == current_client:
                                    self._current_client = None
                                yield self.connecting_currently(util.wait(0))
                                continue

                            if self.running:
                                logger.info('Connected to ZooKeeper ensemble [{0}] with handle [{1}]'.format(server_list, self._current_client.handle))
                            return

                        yield self.connecting_currently(util.wait(0))

                    if not self.running:
                        return
                except defer.CancelledError as ce:
                    raise
                except Exception as e:
                    logger.error('Error while starting ZooKeeper client [{0}]. Will retry. Traceback follows'.format(self), exc_info=True)
                # if we didn't manage to connect, retry with the server list again
                logger.info('Exhausted server list combinations, retrying after 5 seconds.')
                if not self.running:
                    return
                yield self.connecting_currently(util.wait(5))
        except defer.CancelledError as ce:
            pass

    def _create_client(self, servers):
        zk = ZookeeperClient(servers=servers, session_timeout=self.session_timeout)
        zk.set_session_callback(self._watch_connection)
        return zk

    def _started(self, client):
        if client != self._current_client:
            return

        self.cached_get_children = self._cached(client.get_children_and_watch)
        self.cached_get = self._cached(client.get_and_watch)
        self.cached_exists = self._cached(client.exists_and_watch)

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
        if client != self._current_client and client.connected:
            client.close()

        if client != self._current_client or event.path != '':
            return

        # see client.STATE_NAME_MAPPING for possible values for event.state_name
        if event.state_name == 'connected':
            self._cache.clear()
            self.on_connected(self)
            self._on_event('reconnected')

        elif event.state_name == 'connecting':
            # if we're in "connecting" for too long, give up and give us a new connection, the working server list might have changed.
            self.on_disconnected(failure.Failure(DisconnectException(event.state_name)))
            self._on_event('reconnecting')

            if not self.reuse_session and self._current_client:
                logger.info('[{0}] is reconnecting with a new client in order to avoid reusing sessions.'.format(self))
                yield self.stopService()
                yield self.startService()
                return

            self._restart_if_still_running_and_not_connected_after_connect_timeout(self._current_client)

        elif event.state_name == 'expired':
            self.on_disconnected(failure.Failure(DisconnectException(event.state_name)))
            self._on_event(event.state_name)
            # force a full reconnect in order to ensure we get a new session
            yield self.stopService()
            yield self.startService()

        else:
            logger.warn('Unhandled event: {0}'.format(event))

    @defer.inlineCallbacks
    def _restart_if_still_running_and_not_connected_after_connect_timeout(self, client):
        try:
            yield self.reconnecting_currently(util.wait(self.reconnect_timeout))

            if not client == self._current_client:
                return

            if client.state == zookeeper.CONNECTED_STATE:
                return

            logger.info('[0] has been stuck in the connecting state for too long, restarting.')
            yield self.reconnecting_currently(self.stopService())
            yield self.reconnecting_currently(self.startService())
        except defer.CancelledError as ce:
            pass

    def startService(self):
        if not self.running:
            service.Service.startService(self)
            self._on_event('starting')
            return self._start_connecting()

    def stopService(self):
        if self.running:
            service.Service.stopService(self)

            self._on_event('stopping')

            # if we're currently trying to reconnect, stop trying
            if self._currently_reconnecting:
                self._currently_reconnecting.cancel()

            # if we're currently trying to connect, stop trying
            if self._currently_connecting:
                self._currently_connecting.cancel()

            # if we have a client, try to close it, as it might be functional
            if self._current_client:
                defer.maybeDeferred(self._current_client.close).addErrback(lambda _: None)
                self._current_client = None

            self.on_disconnected(failure.Failure(DisconnectException('stopping service')))

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

    def __getattr__(self, item):
        client = self._current_client

        if not client:
            raise zookeeper.ClosingException()

        return getattr(client, item)