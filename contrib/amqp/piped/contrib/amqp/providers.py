# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import copy

import pika
from pika import connection, exceptions as pika_exceptions
from pika.adapters import twisted_connection
from zope import interface
from twisted.application import service
from twisted.internet import reactor, defer, task, endpoints, error
from twisted.python import failure

from piped import log, resource, util, event, exceptions


class AMQPConnectionProvider(object, service.MultiService):
    """ Provides AMQP connections.

    Example:

    .. code-block:: yaml

        amqp:
            connections:
                connection_name:
                    servers:
                        - tcp:host=localhost:port=5672
                    max_idle_time: 10 # reconnect if idle more than 10 seconds
                    parameters:
                        heartbeat: 3 # heartbeat every 3 seconds

    The above configuration will result in a connected :class:`AMQProtocol` being
    provided as ``amqp.connection.connection_name``.

    See :class:`AMQPConnection` for more details on the configuration options.

    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        self._connection_by_name = dict()
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        
        connections = runtime_environment.get_configuration_value('amqp.connections', dict())

        for connection_name, connection_config in connections.items():
            # basic consumers are handled by the AMQPConsumerProvider:
            connection_config = copy.copy(connection_config)
            connection_config.pop('basic_consumers', None)

            connection = AMQPConnection(connection_name, **connection_config)
            connection.setServiceParent(self)
            self._connection_by_name[connection_name] = connection

            logical_name = 'amqp.connection.{0}'.format(connection_name)

            runtime_environment.resource_manager.register(logical_name, provider=self)

    def add_consumer(self, resource_dependency):
        name = resource_dependency.provider.rsplit('.', 1)[-1]
        connection = self._connection_by_name[name]

        connection.on_connected += resource_dependency.on_resource_ready
        connection.on_disconnected += resource_dependency.on_resource_lost

        if connection.ready:
            resource_dependency.on_resource_ready(connection.protocol)


class AMQProtocol(twisted_connection.TwistedProtocolConnection):
    """ The AMQP protocol used by Piped. """

    def __init__(self, parameters):
        super(AMQProtocol, self).__init__(parameters)
        self.on_lost = event.Event()

        self._idle_state = (None, None)
        self._named_channels = dict()
        self._pending_named_channels = dict()
        self.idle_checker = task.LoopingCall(self._check_idle)

    def _adapter_disconnect(self):
        super(AMQProtocol, self)._adapter_disconnect()
        self._check_state_on_disconnect()

    def connectionLost(self, reason):
        self.on_lost(reason)
        # stop the idle checker if it is running
        self.idle_checker.stop() if self.idle_checker.running else None
        return super(AMQProtocol, self).connectionLost(reason)

    def _check_idle(self):
        state = self.bytes_sent, self.bytes_received

        if not state == self._idle_state:
            self._idle_state = state
            return

        if self.connection_state not in (connection.CONNECTION_CLOSED, connection.CONNECTION_CLOSING):
            self.close(320, 'Too long without data transferred.')
            self.transport.loseConnection()

    @defer.inlineCallbacks
    def get_named_channel(self, channel_name):
        """ Utility method that provides easy access to shared channels.

        :param channel_name: A logical named used to identify the channel.
        :return: A deferred that callbacks with the channel or errbacks with
            a failure that describes the reason the channel is unavailable.
        """
        if channel_name in self._named_channels:
            defer.returnValue(self._named_channels[channel_name])

        if channel_name in self._pending_named_channels:
            d = defer.Deferred()
            self._pending_named_channels[channel_name] += d.callback
            channel = yield d
            defer.returnValue(channel)

        self._pending_named_channels[channel_name] = pending_event = event.Event()

        try:
            self._named_channels[channel_name] = channel = yield self.channel()
            channel.add_on_close_callback(lambda: self._named_channels.pop(channel_name, None))
            pending_event(channel)
            defer.returnValue(channel)
        except Exception as e:
            pending_event(failure.Failure())
            raise
        finally:
            self._pending_named_channels.pop(channel_name, None)


class AMQPConnection(object, service.MultiService):
    """ AMQP connection wrapper. """
    ready = False
    protocol = None
    _connecting = None
    _reconnecting = None
    bytes_sent = 0
    bytes_received = 0

    def __init__(self, name, servers, max_idle_time=None, reconnect_interval=1, parameters=None):
        """
        :param name: Logical name of the connection.
        :param servers: List of endpoints to connect to. The endpoints are tried in a round-robin fashion.
            See `twisted.internet.endpoints.clientFromString
            <http://twistedmatrix.com/documents/current/api/twisted.internet.endpoints.html#clientFromString>`_
            for details on the format.

            Example: ``tcp:host=localhost:port=5672``

        :param max_idle_time: The time (in seconds) the connection can be idle (not sending or receiving data)
            before it is forcibly closed.
        :param reconnect_interval: How long to sleep before reconnecting.
        :param parameters: Used to create the connection parameters.
            See the `pika documenation <http://pika.github.com/connecting.html#connection-parameters>`_.
        """
        service.MultiService.__init__(self)
        
        self.name = name
        self.servers = servers
        self._server_index = -1
        self.reconnect_interval = reconnect_interval
        
        parameters = parameters or dict()
        self.parameters = pika.ConnectionParameters(**parameters)

        self.on_connected = event.Event()
        self.on_disconnected = event.Event()

        self.on_connected += lambda _: setattr(self, 'ready', True)
        self.on_disconnected += lambda _: setattr(self, 'ready', False)
        self.on_disconnected += lambda _: setattr(self, 'protocol', None)

        self.max_idle_time = max_idle_time

    def get_next_server(self):
        self._server_index = (self._server_index+1)%len(self.servers)
        return self.servers[self._server_index]

    def configure(self, runtime_environment):
        pass

    def buildProtocol(self, transport):
        protocol = AMQProtocol(self.parameters)
        protocol.factory = self
        return protocol

    def startService(self):
        if not self.running:
            service.MultiService.startService(self)

            self._keep_connecting()

    @defer.inlineCallbacks
    def stopService(self):
        if self.running:
            service.MultiService.stopService(self)
            yield self._disconnect('stopping service')

    @defer.inlineCallbacks
    def _keep_connecting(self):
        currently = util.create_deferred_state_watcher(self, '_reconnecting')
        if self._reconnecting:
            return

        while self.running:
            try:
                server = self.get_next_server()
                log.info('Connecting to {0}'.format(server))

                endpoint = endpoints.clientFromString(reactor, server)

                yield currently(self._connect(endpoint))

            except defer.CancelledError as ce:
                # the connection attempt might be cancelled, due to stopService/_disconnect
                break

            except (error.ConnectError, error.DNSLookupError, error.ConnectionDone) as ce:
                # ConnectionDone might be raised because we managed to connect, but reached the
                # max idle time during the AMQP handshake.
                log.warn('Unable to connect: {0}'.format(repr(ce)))
                self.on_disconnected(failure.Failure())
                yield currently(util.wait(self.reconnect_interval))
                continue

            except Exception as e:
                # log any completely unexpected errors, wait a little, then retry.
                log.warn()
                self.on_disconnected(failure.Failure())
                yield currently(util.wait(self.reconnect_interval))
                continue

            else:
                break

    @defer.inlineCallbacks
    def _connect(self, endpoint):
        currently = util.create_deferred_state_watcher(self, '_connecting')
        
        # make sure we're not directly overwriting an existing protocol
        if self.protocol:
            if self.protocol.connection_state not in (connection.CONNECTION_CLOSED, connection.CONNECTION_CLOSING):
                self._disconnect('reconnecting')

        # once twisted has connected to the other side, we set our protocol
        self.protocol = protocol = yield currently(endpoint.connect(self))
        self.protocol.idle_checker.start(self.max_idle_time, now=False) if self.max_idle_time is not None else None

        yield currently(self.protocol.ready)

        self.on_connected(self.protocol)

        def on_lost(reason):
            protocol.on_lost -= on_lost
            if self.protocol == protocol:
                if protocol.idle_checker.running:
                    protocol.idle_checker.stop()
                # set the state to disconnected if we're still the current protocol
                self.on_disconnected(reason)
                # reconnect automatically if we're still supposed to be the current protocol
                self._keep_connecting()
                
        self.protocol.on_lost += on_lost

    def _disconnect(self, reason='unknown'):
        if self._reconnecting:
            self._reconnecting.cancel()
        if self._connecting:
            self._connecting.cancel()

        if self.protocol:
            if not self.protocol.connection_state in (connection.CONNECTION_CLOSED, connection.CONNECTION_CLOSING):
                self.protocol.close(200, reason)

            if self.protocol.idle_checker.running:
                self.protocol.idle_checker.stop()

        self.on_disconnected(reason)


class AMQPConsumerProvider(object, service.MultiService):
    """ Consumes message from AMQP queues.

    Example configuration:

    .. code-block:: yaml

        amqp:
            connections:
                connection_name:
                    basic_consumers:
                        consumer_name:
                            queue: name_of_queue_to_consume
                            qos: # see http://www.rabbitmq.com/amqp-0-9-1-quickref.html#basic.qos
                                prefetch_count: 200


    See :class:`AMQPConsumer` for more details about available consumer configuration options.
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        self._consumer_by_name = dict()
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)

        connection_configs = runtime_environment.get_configuration_value('amqp.connections', dict())

        for connection_name, connection_config in connection_configs.items():
            consumers = connection_config.get('basic_consumers', dict())

            for consumer_name, consumer_config in consumers.items():
                consumer = AMQPConsumer(name=consumer_name, connection=connection_name, **consumer_config)

                consumer.configure(runtime_environment)
                consumer.setServiceParent(self)

                self._consumer_by_name[consumer_name] = consumer


class AMQPConsumer(object, service.Service):
    """ Consumes messages from an AMQP queue and processes them in a pipeline. """
    _working = None

    def __init__(self, name, pipeline, connection, queue=None, qos=None,
        ack_before_processing=False, ack_after_successful_processing=True,
        nack_after_failed_processing=True, channel_reopen_interval=1):
        """
        :param name: Logical name of this consumer.
        :param pipeline: The pipeline used to process the messages.
        :param connection: Name of the connection.
        :param queue: Either a name (string) or a queue declaration (dict).
            See `queue_declare <http://www.rabbitmq.com/amqp-0-9-1-quickref.html#queue.declare>`_.
        :param qos: Used to specify the QOS on the channel.
        :param ack_before_processing: If true, acks messages before they are processed.
        :param ack_after_successful_processing: If true, acks messages if processing finishes
            without errbacking.
        :param nack_after_failed_processing: If true, rejects messages if processing errbacks.
        :param channel_reopen_interval: Time (in seconds) to wait before reopening the consuming
            channel if it closes.
        """
        self.name = name
        self.pipeline_name = pipeline
        self.connection_name = connection

        if isinstance(queue, basestring):
            queue = dict(queue=queue, passive=True)

        self.queue_declare = queue or dict()
        self.queue_declare.setdefault('queue', '')
        self.queue_declare.setdefault('auto_delete', True)
        self.queue_declare.setdefault('durable', False)

        # default the queue to exclusive if the queue name is auto-generated
        self.queue_declare.setdefault('exclusive', True if not self.queue_declare['queue'] else False)

        self.qos = qos or dict()

        self.ack_before_processing = ack_before_processing
        self.ack_after_successful_processing = ack_after_successful_processing
        self.nack_after_failed_processing = nack_after_failed_processing

        if self.ack_before_processing and (self.ack_after_successful_processing or self.nack_after_failed_processing):
            e_msg = 'Cannot both ack before processing and ack/nack after processing.'
            raise exceptions.ConfigurationError(e_msg)

        self.channel_reopen_interval = channel_reopen_interval

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager

        self.dependency = dm.as_dependency(self)
        self.dependency.on_ready += lambda _: self._run()
        self.dependency.on_lost += lambda _, reason: self._stop_working()

        self.pipeline_dependency = dm.add_dependency(self, dict(provider='pipeline.{0}'.format(self.pipeline_name)))
        self.connection_dependency = dm.add_dependency(self, dict(provider='amqp.connection.{0}'.format(self.connection_name)))

    def _stop_working(self):
        if self._working:
            self._working.cancel()

    @defer.inlineCallbacks
    def _run(self):
        currently = util.create_deferred_state_watcher(self, '_working')
        if self._working:
            # we only need one worker :)
            return

        while self.running:
            try:
                connection = yield currently(self.connection_dependency.wait_for_resource())
                channel = yield currently(connection.channel())

                if self.qos:
                    yield currently(channel.basic_qos(**self.qos))

                frame = yield currently(channel.queue_declare(**self.queue_declare))
                queue_name = frame.method.queue

                queue, consumer_tag = yield currently(channel.basic_consume(queue=queue_name))

                while self.running:
                    channel, method, properties, body = yield currently(queue.get())

                    if self.ack_before_processing:
                        yield currently(channel.basic_ack(delivery_tag=method.delivery_tag))

                    self._process(channel, method, properties, body)

            except pika_exceptions.ChannelClosed as cc:
                log.warn()
                yield util.wait(self.channel_reopen_interval)

            except Exception as e:
                log.warn()

    @defer.inlineCallbacks
    def _process(self, channel, method, properties, body):
        try:
            yield self.process(channel=channel, method=method, properties=properties, body=body)
        except Exception as e:
            log.warn()
            if self.nack_after_failed_processing:
                yield channel.basic_reject(delivery_tag=method.delivery_tag)
        else:
            if self.ack_after_successful_processing:
                yield channel.basic_ack(delivery_tag=method.delivery_tag)

    @defer.inlineCallbacks
    def process(self, **baton):
        pipeline = yield self.pipeline_dependency.wait_for_resource()
        yield pipeline.process(baton)

    def startService(self):
        if not self.running:
            service.Service.startService(self)
        if self.dependency.is_ready:
            self._run()

    def stopService(self):
        service.Service.stopService(self)
        self._stop_working()
