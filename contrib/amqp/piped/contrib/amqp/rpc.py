# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from zope import interface
from pika import exceptions as pika_exceptions
from twisted.application import service
from twisted.internet import defer, reactor
from twisted.python import reflect

from piped import log, resource, util, event, dependencies


class RPCClientProvider(object, service.MultiService):
    """ Provides AMQP-based RPC clients.

    Configuration example:

    .. code-block:: yaml

        amqp:
            rpc_clients:
                my_client:
                    type: your_package.your_module.RPCClient
                    consumer:
                        queue:
                            queue: explicit_queue_name
                            exclusive: true

    See :class:`RPCClientBase`, which may serve as an useful base-class.
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._client_by_name = dict()

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager

        self.clients = runtime_environment.get_configuration_value('amqp.rpc_clients', dict())
        resource_manager = runtime_environment.resource_manager

        for client_name, client_configuration in self.clients.items():
            resource_manager.register('amqp.rpc_client.{0}'.format(client_name), provider=self)

            self._get_or_create_client(client_name)

    def _get_or_create_client(self, client_name):
        if client_name not in self._client_by_name:
            client_configuration = self.clients[client_name]
            type = client_configuration.pop('type', reflect.fullyQualifiedName(RPCClientBase))
            client = reflect.namedAny(type)(client_name, **client_configuration)
            client.configure(self.runtime_environment)
            client.setServiceParent(self)

            self._client_by_name[client_name] = client
        return self._client_by_name[client_name]

    def add_consumer(self, resource_dependency):
        client_name = resource_dependency.provider.rsplit('.', 1)[-1]
        client = self._get_or_create_client(client_name)
        client_dependency = self.dependency_manager.as_dependency(client)

        client_dependency.on_ready += lambda dep: resource_dependency.on_resource_ready(dep.get_resource())
        client_dependency.on_lost += lambda dep, reason: resource_dependency.on_resource_lost(reason)

        # the client might already be ready:
        if client_dependency.is_ready:
            resource_dependency.on_resource_ready(client)


class RPCClientBase(object, service.Service):
    """ Base class for RPC client implementations.

    """
    connection = None
    consume_channel = None
    response_queue = None
    publish_channel = None
    pending_publish_channel = None

    _starting = None
    _consuming = None

    def __init__(self, name, connection, reopen_consumer_channel_interval=1, consumer=None):
        """
        :param name: Logical name of this rpc client.
        :param connection: Name of the AMQP connection.
        :param consumer: Configuration options for the consuming:
            
            no_ack
                Whether the server should require the consumer to ack the responses. Defaults to True.

            exclusive
                Request exclusive consumer access, meaning only this consumer can access the queue. Defaults to True.

            queue
                Name of the response queue (string) or a queue declaration (dict). Defaults to
                the empty string, which creates an anonymous queue.
                See `queue_declare <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#queue.declare>`_.
        :param reopen_consumer_channel_interval: The number of seconds to wait before reopening the
            consumer channel if it is closed.
        """
        self.name = name
        self.connection_name = connection
        self.consumer_config = consumer or dict()
        self.consumer_config.setdefault('no_ack', True)
        self.consumer_config.setdefault('exclusive', True)
        
        response_queue = self.consumer_config.pop('queue', None)
        if isinstance(response_queue, basestring):
            response_queue = dict(queue=response_queue)
        self.response_queue_declare = response_queue or dict(queue='')
        self.reopen_consumer_channel_interval = reopen_consumer_channel_interval

        self.on_connection_lost = event.Event()
        self.on_connection_lost += lambda reason: self._handle_disconnect()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = self.runtime_environment.dependency_manager
        dm = self.dependency_manager

        self.dependency = dm.as_dependency(self)

        # create a dependency representing our consuming state, which we will fire
        # on_ready and on_lost ourselves on depending on whether we're ready to consume
        self.consuming_dependency = dependencies.Dependency()
        self.consuming_dependency.cascade_ready = False
        dm.add_dependency(self, self.consuming_dependency)

        self.connection_dependency = dm.add_dependency(self, dict(provider='amqp.connection.{0}'.format(self.connection_name)))
        self.connection_dependency.on_lost += lambda dep, reason: self.on_connection_lost(reason)
        self.connection_dependency.on_ready += lambda dependency: self._consider_starting()

    @defer.inlineCallbacks
    def _consider_starting(self):
        currently = util.create_deferred_state_watcher(self, '_starting')

        if not self.connection_dependency.is_ready:
            return

        if not self.running or self._starting or self._consuming:
            return

        try:
            if not self.connection:
                self.connection = yield currently(self.connection_dependency.wait_for_resource())
            if not self.consume_channel:
                self.consume_channel = yield currently(self.connection.channel())
            if not self.response_queue:
                self.response_queue = yield currently(self.consume_channel.queue_declare(**self.response_queue_declare))

            # we're ready to start consuming now:
            self._consume_queue()
        except defer.CancelledError as ce:
            log.info('Initialization of %r was cancelled.' % self)

    def _handle_disconnect(self):
        self.connection = None
        self.consume_channel = None
        self.response_queue = None

        # cancel any current operations
        for current_operation in (self._starting, self._consuming):
            if current_operation:
                current_operation.cancel()

    def startService(self):
        service.Service.startService(self)
        self._consider_starting()

    def stopService(self):
        service.Service.stopService(self)
        
        for current_operation in (self._starting, self._consuming):
            if current_operation:
                current_operation.cancel()

    @defer.inlineCallbacks
    def _consume_queue(self):
        currently = util.create_deferred_state_watcher(self, '_consuming')

        queue, consumer_tag = yield currently(self.consume_channel.basic_consume(queue=self.response_queue.method.queue, **self.consumer_config))
        try:
            self.consuming_dependency.fire_on_ready()

            while self.running:
                # don't get from the queue unless our dependency is ready
                yield currently(self.dependency.wait_for_resource())

                channel, method, properties, body = yield currently(queue.get())

                self.process_response(channel, method, properties, body)

        except pika_exceptions.ChannelClosed as cc:
            log.warn()
            self.consuming_dependency.fire_on_lost('channel closed')
            self._handle_disconnect()

            # wait a little before reopening the channel
            reactor.callLater(self.reopen_consumer_channel_interval, self._consider_starting)

        except defer.CancelledError as ce:
            return

        finally:
            self.consuming_dependency.fire_on_lost('stopped consuming')

    def process_response(self, channel, method, properties, body):
        raise NotImplementedError()