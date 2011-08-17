# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.application import internet, service
from twisted.conch import manhole, manhole_ssh, error as conch_error
from twisted.conch.insults import insults
from twisted.conch.ssh import keys
from twisted.cred import error, portal
from twisted.python import reflect
from twisted.internet import defer, reactor
from zope import interface

import zookeeper
from txzookeeper import client

from piped import resource, event, log, exceptions, util


class ZookeeperClientProvider(object, service.MultiService):
    """ Embeds manholes in Piped services.

    Configuration example::

        zookeeper:
            clients:
                my_client:
                    servers: localhost:2181
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._client_by_name = dict()

    def configure(self, runtime_environment):
        self.setName('zookeeper')
        self.setServiceParent(runtime_environment.application)
        self.runtime_environment = runtime_environment

        self.clients = runtime_environment.get_configuration_value('zookeeper.clients', dict())
        resource_manager = runtime_environment.resource_manager

        for client_name, client_configuration in self.clients.items():
            resource_manager.register('zookeeper.client.%s' % client_name, provider=self)
            # create the client if we have any event pipelines
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
    
    def __init__(self, events = None, *a, **kw):
        super(PipedZookeeperClient, self).__init__(*a, **kw)
        self.events = events or dict()

        self.on_connected = event.Event()
        self.on_disconnected = event.Event()

        self.set_session_callback(self._watch_connection)

    def configure(self, runtime_environment):
        for key, value in self.events.items():
            if key not in self.possible_events:
                e_msg = 'Invalid event: {0}.'.format(key)
                detail = 'Use one of the possible events: {0}'.format(self.possible_events)
                raise exceptions.ConfigurationError(e_msg, detail)

            self.events[key] = dict(provider='pipeline.{0}'.format(value))
        
        self.dependencies = runtime_environment.create_dependency_map(self, **self.events)

    def _started(self):
        self.connecting = None
        self.on_connected(self)
        self._on_event('connected')

    @defer.inlineCallbacks
    def _on_event(self, event_name):
        baton = dict(event=event_name, client=self)

        try:
            pipeline = yield self.dependencies.wait_for_resource(event_name)
            yield pipeline.process(baton)
        except KeyError as ae:
            # we have no pipeline for this event
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
            self.on_disconnected(event.state_name)
            self._on_event('reconnecting')
        elif event.state_name == 'expired':
            self.on_disconnected(event.state_name)
            self._on_event('expired')
            # force a full reconnect in order to ensure we get a new session
            yield self.stopService()
            yield self.startService()
        else:
            # do nothing for auth failed or associating
            pass

    def startService(self):
        if not self.running:
            service.Service.startService(self)
            self._on_event('starting')
            if self.connecting:
                log.warn('Started connecting before previous connect finished.')
                return
            # TODO: what if I have to stop/start during connecting?
            self.connecting = self.connect(timeout=60*60*24*365)
            return self.connecting.addCallback(lambda _: self._started())

    def stopService(self):
        if self.running:
            service.Service.stopService(self)
            self.on_disconnected('stopping')
            self._on_event('stopping')
            return self.close()