# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import mock
from mock import patch
from pika import exceptions as pika_exceptions
from twisted.application import service
from twisted.python import reflect, failure
from twisted.trial import unittest
from twisted.internet import defer

from piped import processing, dependencies, util
from piped_amqp import rpc


class StubRPCClient(rpc.RPCClientBase):
    pass


class TestRPCClientProvider(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.configure()

    def tearDown(self):
        self.runtime_environment.application.stopService()

    def test_provided_connections(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('amqp.rpc_clients.test_client', dict(
            connection = 'test_connection',
            type = reflect.fullyQualifiedName(StubRPCClient),
            consumer = dict(
                queue = dict(
                    queue = 'my_queue'
                )
            )
        ))

        provider = rpc.RPCClientProvider()
        provider.configure(self.runtime_environment)

        client_dependency = dependencies.ResourceDependency(provider='amqp.rpc_client.test_client')
        self.runtime_environment.resource_manager.resolve(client_dependency)

        mocked_connection = mock.Mock()
        mocked_connection.channel.return_value.basic_consume.return_value = defer.DeferredQueue(), mock.Mock()

        with patch.object(self.runtime_environment.resource_manager, 'resolve') as mocked_resolve:
            def resolve(resource_dependency):
                self.assertEquals(resource_dependency.provider, 'amqp.connection.test_connection')
                resource_dependency.on_resource_ready(mocked_connection)

            # replace 'amqp.connection' with the mocked connection:
            mocked_resolve.side_effect = resolve

            self.runtime_environment.dependency_manager.resolve_initial_states()
            provider.startService()

        client = client_dependency.get_resource()
        self.assertIsInstance(client, StubRPCClient)

        self.assertEquals(client.response_queue_declare['queue'], 'my_queue')


        # if another dependency asks for the same connection, it should receive the same instance
        another_client_dependency = dependencies.ResourceDependency(provider='amqp.rpc_client.test_client')
        self.runtime_environment.resource_manager.resolve(another_client_dependency)

        self.assertIdentical(another_client_dependency.get_resource(), client_dependency.get_resource())


class TestRPC(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.configure()

    def tearDown(self):
        return self.runtime_environment.application.stopService()

    def _create_rpc_client(self, **rpc_config):
        dm = self.runtime_environment.dependency_manager

        rpc_config.setdefault('name', 'test_name')
        rpc_config.setdefault('connection', 'test_connection')

        rpc_client = rpc.RPCClientBase(**rpc_config)
        rpc_client.configure(self.runtime_environment)

        dm._dependency_graph.node[rpc_client.connection_dependency]['resolved'] = True
        dm._dependency_graph.node[rpc_client.consuming_dependency]['resolved'] = True
        dm._dependency_graph.node[rpc_client.dependency]['resolved'] = True

        return rpc_client

    def test_connect_disconnect_when_service_starts_stops(self):
        dm = self.runtime_environment.dependency_manager
        rpc_client = self._create_rpc_client()

        mocked_connection = mock.Mock()
        mocked_channel = mocked_connection.channel.return_value = mock.Mock()

        message_queue = defer.DeferredQueue()
        mocked_consumer_tag = mock.Mock()
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)

        # the service isn't started yet
        self.assertFalse(rpc_client.dependency.is_ready)

        # starting the service should make the rpc-clients dependency ready
        rpc_client.startService()
        self.assertTrue(rpc_client.dependency.is_ready)
        self.assertIsInstance(rpc_client._consuming, defer.Deferred)

        with patch.object(rpc_client, 'process_response') as mocked_response:
            mocked_method, mocked_properties = mock.Mock(name='method'), mock.Mock(name='properties')
            message_queue.put((mocked_channel, mocked_method, mocked_properties, 'test message body'))

            args = mocked_response.call_args[0]
            self.assertEquals(args[0], mocked_channel)
            self.assertEquals(args[1], mocked_method)
            self.assertEquals(args[2], mocked_properties)
            self.assertEquals(args[3], 'test message body')

        # when the connection becomes lost, the rpc client should
        # stop consuming
        rpc_client.connection_dependency.on_resource_lost('test lost')
        self.assertFalse(rpc_client.connection_dependency.is_ready)

        #
        self.assertNot(rpc_client._consuming)
        self.assertFalse(rpc_client.dependency.is_ready)

    def test_availability_on_connection_lost(self):
        dm = self.runtime_environment.dependency_manager
        rpc_client = self._create_rpc_client()

        mocked_connection = mock.Mock()
        mocked_channel = mocked_connection.channel.return_value = mock.Mock()

        message_queue = defer.DeferredQueue()
        mocked_consumer_tag = mock.Mock()
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)

        # the client is not ready yet, since it isn't consuming
        self.assertFalse(rpc_client.dependency.is_ready)

        # starting the service should make the rpc-clients dependency ready
        rpc_client.startService()
        self.assertTrue(rpc_client.dependency.is_ready)
        self.assertIsInstance(rpc_client._consuming, defer.Deferred)

        # when the connection becomes lost, the rpc client should
        # stop consuming
        rpc_client.connection_dependency.on_resource_lost('test lost')
        self.assertFalse(rpc_client.connection_dependency.is_ready)
        self.assertFalse(rpc_client.dependency.is_ready)
        self.assertNot(rpc_client._consuming)

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)
        self.assertTrue(rpc_client.connection_dependency.is_ready)
        self.assertTrue(rpc_client.dependency.is_ready)
        self.assertIsInstance(rpc_client._consuming, defer.Deferred)

        self.assertEquals(mocked_channel.basic_consume.call_count, 2)

    def test_availability_on_start_stop_service(self):
        dm = self.runtime_environment.dependency_manager
        rpc_client = self._create_rpc_client()

        mocked_connection = mock.Mock()
        mocked_channel = mocked_connection.channel.return_value = mock.Mock()

        message_queue = defer.DeferredQueue()
        mocked_consumer_tag = mock.Mock()
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)

        # the service isn't started yet:
        self.assertFalse(rpc_client.dependency.is_ready)

        # starting the service should make the rpc-clients dependency ready
        rpc_client.startService()
        self.assertTrue(rpc_client.dependency.is_ready)
        self.assertIsInstance(rpc_client._consuming, defer.Deferred)

        # when the service stops, the consumer client should stop consuming
        rpc_client.stopService()
        self.assertTrue(rpc_client.connection_dependency.is_ready)
        self.assertFalse(rpc_client.dependency.is_ready)
        self.assertNot(rpc_client._consuming)

        rpc_client.startService()
        self.assertTrue(rpc_client.connection_dependency.is_ready)
        self.assertTrue(rpc_client.dependency.is_ready)
        self.assertIsInstance(rpc_client._consuming, defer.Deferred)

        self.assertEquals(mocked_channel.basic_consume.call_count, 2)

    def test_consumer_configuration_with_acking(self):
        rpc_client = self._create_rpc_client(consumer=dict(no_ack=False))

        mocked_connection = mock.Mock()
        mocked_channel = mocked_connection.channel.return_value = mock.Mock()
        mocked_channel.queue_declare.return_value.method.queue = 'test-amqp-generated-queue-name'

        message_queue = defer.DeferredQueue()
        mocked_consumer_tag = mock.Mock()
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)
        rpc_client.startService()

        # the consumer should use the generated queue name when starting to consume
        mocked_channel.basic_consume.assert_called_once_with(
            queue = 'test-amqp-generated-queue-name',
            no_ack = False,
            exclusive = True
        )

    def test_consumer_configuration_custom_queue_name(self):
        rpc_client = self._create_rpc_client(consumer=dict(queue='my_queue'))

        mocked_connection = mock.Mock()
        mocked_channel = mocked_connection.channel.return_value = mock.Mock()

        def queue_declare(queue, **rest):
            m = mock.Mock()
            m.method.queue = queue
            return m

        mocked_channel.queue_declare.side_effect = queue_declare

        message_queue = defer.DeferredQueue()
        mocked_consumer_tag = mock.Mock()
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)
        rpc_client.startService()

        # the consumer should use the configured queue name when starting to consume
        mocked_channel.basic_consume.assert_called_once_with(
            queue = 'my_queue',
            no_ack = True,
            exclusive = True
        )

    @defer.inlineCallbacks
    def test_channel_reopened_if_closed(self):
        rpc_client = self._create_rpc_client(reopen_consumer_channel_interval=0)

        mocked_connection = mock.Mock()
        mocked_channel = mocked_connection.channel.return_value = mock.Mock()

        message_queue = defer.DeferredQueue()
        mocked_consumer_tag = mock.Mock()
        mocked_channel.basic_consume.return_value = message_queue, mocked_consumer_tag

        rpc_client.connection_dependency.on_resource_ready(mocked_connection)
        rpc_client.startService()

        with patch.object(rpc_client, 'process_response') as mocked_response:
            with patch.object(rpc.log, 'warn') as mocked_warn:
                message_queue.put(failure.Failure(pika_exceptions.ChannelClosed('test closed')))
                self.assertEquals(mocked_warn.call_count, 1)

        # the channel closed exception should stop the consuming
        self.assertEquals(rpc_client.consuming_dependency.is_ready, False)
        self.assertEquals(len(message_queue.waiting), 0)

        # wait for the reopen_consumer_channel_interval to pass
        yield util.wait(0)

        # .. the consuming should have been restarted:
        self.assertEquals(rpc_client.consuming_dependency.is_ready, True)
        self.assertEquals(len(message_queue.waiting), 1)
