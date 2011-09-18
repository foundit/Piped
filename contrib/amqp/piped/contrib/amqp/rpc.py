# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.application import service
from twisted.internet import defer
from zope import interface
from twisted.python import reflect

from piped import log, resource, util, event


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

    def __init__(self, name, connection, consumer=None):
        """
        :param name: Logical name of this rpc client.
        :param connection: Name of the AMQP connection.
        :param consumer: Configuration options for the consuming:
            
            no_ack
                Whether the server should require the consumer to ack the responses. Defaults to false.

            queue
                Name of the response queue (string) or a queue declaration (dict). Defaults to
                the empty string, which creates an anonymous queue.
                See `queue_declare <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#queue.declare>`_.
        """
        self.name = name
        self.connection_name = connection
        self.consumer_config = consumer or dict()
        
        response_queue = self.consumer_config.get('queue', None)
        if isinstance(response_queue, basestring):
            response_queue = dict(queue=response_queue)

        self.response_queue_declare = response_queue or dict()
        self.response_queue_declare.setdefault('queue', '')
        self.response_queue_declare.setdefault('auto_delete', True)
        self.response_queue_declare.setdefault('durable', False)
        self.response_queue_declare.setdefault('exclusive', True)

        self.requests = dict()
        self.on_connection_lost = event.Event()
        self.on_connection_lost += lambda reason: self._handle_disconnect()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = self.runtime_environment.dependency_manager
        dm = self.dependency_manager

        self.dependency = dm.as_dependency(self)
        self.dependency.cascade_ready = False
        self.dependency.on_dependency_ready += lambda dependency: self._consider_starting()

        self.connection_dependency = dm.add_dependency(self, dict(provider='amqp.connection.{0}'.format(self.connection_name)))
        self.connection_dependency.on_lost += lambda dep, reason: self.on_connection_lost(reason)

    @defer.inlineCallbacks
    def _consider_starting(self):
        currently = util.create_deferred_state_watcher(self, '_starting')

        if not self.dependency_manager.has_all_dependencies_provided(self.dependency):
            return

        if not self.running or self._starting:
            return

        try:
            if not self.connection:
                self.connection = yield currently(self.connection_dependency.wait_for_resource())
            if not self.consume_channel:
                self.consume_channel = yield currently(self.connection.channel())
            if not self.response_queue:
                self.response_queue = yield currently(self.consume_channel.queue_declare(**self.response_queue_declare))

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

        self.dependency.fire_on_lost('stopping client')

    @defer.inlineCallbacks
    def _consume_queue(self):
        currently = util.create_deferred_state_watcher(self, '_consuming')
        
        queue, consumer_tag = yield currently(self.consume_channel.basic_consume(queue=self.response_queue.method.queue, no_ack=self.consumer_config.get('no_ack', True), exclusive=self.response_queue_declare['exclusive']))
        if self.running:
            self.dependency.fire_on_ready()
        try:
            while self.dependency.is_ready and self.running:
                channel, method, properties, body = yield currently(queue.get())
                d = defer.maybeDeferred(self.process_response, channel, method, properties, body)
        except defer.CancelledError as ce:
            return

    def process_response(self, channel, method, properties, body):
        raise NotImplementedError()