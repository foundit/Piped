# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import mocker
import zmq
from twisted.application import service
from twisted.internet import defer
from twisted.trial import unittest

from piped import log, processing, dependencies, util
from piped_zmq import providers


class StubSocketPoller(providers.ZMQSocketPoller):

    def __init__(self, register_callback):
        self.register_callback = register_callback

    def register_socket(self, feeder, socket):
        self.register_callback(feeder, socket)


class ZMQSocketProviderTest(unittest.TestCase):

    def setUp(self):
        self.mocker = mocker.Mocker()
        self.runtime_environment = processing.RuntimeEnvironment()
        self.resource_manager = self.runtime_environment.resource_manager
        self.dependency_manager = self.runtime_environment.dependency_manager
        self.runtime_environment.dependency_manager.configure(self.runtime_environment)

        # We're going to fake the configuration values under here.
        self.runtime_environment.configuration_manager.set('zmq', dict())

    def tearDown(self):
        self.mocker.restore()

    def test_getting_a_socket_with_an_invalid_name_raises_exception(self):
        socket_provider = providers.ZMQSocketProvider()
        socket_provider.configure(self.runtime_environment)
        resource_dependency = dependencies.ResourceDependency(provider='zmq.socket.no_such_socket')
        self.assertRaises(providers.NoSuchSocketError, socket_provider.add_consumer, resource_dependency)

    def test_that_sockets_are_provided(self):
        socket_provider = providers.ZMQSocketProvider()
        queues = dict(
            binder=dict(type='PUSH', binds=['inproc://socketname']),
            connecter=dict(type='PULL', connects=['inproc://socketname']),
        )
        self.runtime_environment.configuration_manager.set('zmq.queues', queues)

        resource_manager = self.runtime_environment.resource_manager = self.mocker.mock()
        resource_manager.register('zmq.context', provider=socket_provider)
        resource_manager.register('zmq.socket.binder', provider=socket_provider)
        resource_manager.register('zmq.socket.connecter', provider=socket_provider)

        self.mocker.replay()
        socket_provider.configure(self.runtime_environment)
        self.mocker.verify()

    def test_no_queues(self):
        resource_manager = self.runtime_environment.resource_manager = self.mocker.mock()
        socket_provider = providers.ZMQSocketProvider()

        # Only the context should be provided:
        resource_manager.register('zmq.context', provider=socket_provider)

        self.mocker.replay()
        socket_provider.configure(self.runtime_environment)
        self.mocker.verify()

    def test_invalid_socket_types_are_reported(self):
        self.runtime_environment.configuration_manager.set('zmq.queues', dict(socket_name=dict(type='INVALID TYPE')))
        socket_provider = providers.ZMQSocketProvider()
        self.assertRaises(providers.SocketError, socket_provider.configure, self.runtime_environment)

    def test_binding_and_connecting(self):
        queues = dict(
            connect_and_bind=dict(type='PUSH', binds=['inproc://socketname'], connects=['tcp://0.0.0.0:1234']),
            multiple_connects=dict(type='PUSH', connects=['tcp://0.0.0.0:1234', 'tcp://0.0.0.0:1235']),
            multiple_binds=dict(type='PUSH', binds=['tcp://0.0.0.0:1234', 'tcp://0.0.0.0:1235']),
            no_connects_nor_binds=dict(type='PUSH')
        )
        self.runtime_environment.configuration_manager.set('zmq.queues', queues)

        socket_provider = providers.ZMQSocketProvider()
        context = self.mocker.mock()
        socket_provider.context_factory = lambda: context
        sockets = dict((queue_name, self.mocker.mock()) for queue_name in queues)

        for queue_name in queues:
            # Make the context return the right socket-mocks.
            mocked_socket = sockets[queue_name]
            context.socket(zmq.PUSH)
            self.mocker.result(mocked_socket)

            for bind_spec in queues[queue_name].get('binds', []):
                mocked_socket.bind(bind_spec)

            for connect_spec in queues[queue_name].get('connects', []):
                mocked_socket.connect(connect_spec)

        self.mocker.replay()

        socket_provider.configure(self.runtime_environment)

        for queue_name in queues:
            resource_dependency = dependencies.ResourceDependency(provider='zmq.socket.%s'%queue_name)
            socket_provider.add_consumer(resource_dependency)

        socket_provider.start_providing()

        self.mocker.verify()

    @defer.inlineCallbacks
    def test_sockets_of_the_same_name_are_reused(self):
        self.runtime_environment.configuration_manager.set('zmq.queues', dict(socketname=dict(type='PULL', binds=['inproc://socketname'])))
        socket_provider = providers.ZMQSocketProvider()
        socket_provider.configure(self.runtime_environment)

        # Request the same socket twice.
        resource_dependency = dependencies.ResourceDependency(provider='zmq.socket.socketname')
        same_resource_dependency = dependencies.ResourceDependency(provider='zmq.socket.socketname')

        # The two resource configuration-dicts now have on_available-events.
        sockets = []
        resource_dependency.on_resource_ready += lambda resource: sockets.append(resource)
        same_resource_dependency.on_resource_ready += lambda resource: sockets.append(resource)

        # we add them without dependencies, as they will be resolved by the resource manager
        self.dependency_manager.add_dependency(resource_dependency)
        self.dependency_manager.add_dependency(same_resource_dependency)

        # This provides the sockets.
        self.dependency_manager.resolve_initial_states()
        yield util.wait(0)

        # We should then have two sockets
        self.assertEquals(len(sockets), 2)
        # And they must be the same object.
        self.assertTrue(sockets[0] is sockets[1])

    @defer.inlineCallbacks
    def test_binding_before_connecting(self):
        queues = dict(
            binder=dict(type='PUSH', binds=['inproc://socketname']),
            connecter=dict(type='PULL', connects=['inproc://socketname']),
            )

        self.runtime_environment.configuration_manager.set('zmq.queues', queues)

        socket_provider = providers.ZMQSocketProvider()
        # Set up the mocks.
        context = self.mocker.mock()
        socket_provider.context_factory = lambda: context
        binder_socket = self.mocker.mock()
        connecter_socket = self.mocker.mock()

        # First the sockets are requested.
        context.socket(zmq.PUSH)
        self.mocker.result(binder_socket)
        context.socket(zmq.PULL)
        self.mocker.result(connecter_socket)

        with self.mocker.order():
            # Then they are bound/connected
            binder_socket.bind('inproc://socketname')
            connecter_socket.connect('inproc://socketname')

        self.mocker.replay()

        socket_provider.configure(self.runtime_environment)

        # Request the sockets
        connecter_dependency = dependencies.ResourceDependency(provider='zmq.socket.connecter')
        binder_dependency = dependencies.ResourceDependency(provider='zmq.socket.binder')

        socket_by_name = dict()
        connecter_dependency.on_resource_ready += lambda resource:socket_by_name.__setitem__('connecter', resource)
        binder_dependency.on_resource_ready += lambda resource:socket_by_name.__setitem__('binder', resource)

        self.dependency_manager.add_dependency(connecter_dependency)
        self.dependency_manager.add_dependency(binder_dependency)

        self.dependency_manager.resolve_initial_states()

        yield util.wait(0)

        self.mocker.verify()
        self.assertEquals(socket_by_name, dict(binder=binder_socket, connecter=connecter_socket))


class ZMQProcessorFeederProviderTest(unittest.TestCase):

    def setUp(self):
        self.mocker = mocker.Mocker()
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.configuration_manager.set('zmq', dict())

        self.dependency_manager = self.runtime_environment.dependency_manager
        self.dependency_manager.configure(self.runtime_environment)

    def test_that_message_providers_request_the_necessary_sockets(self):
        queues = dict(
            binder=dict(type='PUSH', processor='pipeline.foo', binds=['inproc://socketname']),
            connecter=dict(type='PULL', processor='bar', connects=['inproc://socketname']),
        )

        self.runtime_environment.configuration_manager.set('zmq.queues', queues)

        message_source = providers.ZMQProcessorFeederProvider()
        message_source.configure(self.runtime_environment)

        expected_dependencies = ['zmq.context', 'zmq.socket.binder', 'zmq.socket.connecter', 'pipeline.foo', 'bar']

        # The configure calls should have resulted in the sockets being requested
        for depended_on in [dependency.provider for dependency in self.dependency_manager._dependency_graph.nodes() if hasattr(dependency, 'provider')]:
            self.assertIn(depended_on, expected_dependencies)
            expected_dependencies.remove(depended_on)

        self.assertEquals(expected_dependencies, [], 'The expected dependencies were not requested.')

    def test_no_queues(self):
        self.runtime_environment.dependency_manager = self.mocker.mock()
        self.runtime_environment.resource_manager = self.mocker.mock()
        # since there are no queues defined that uses a processor, no dependencies are added
        self.mocker.replay()

        feeder_provider = providers.ZMQProcessorFeederProvider()
        feeder_provider.configure(self.runtime_environment)

        self.mocker.verify()


class ZMQProcessorFeederTest(unittest.TestCase):

    def setUp(self):
        self.mocker = mocker.Mocker()
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()

        self.dependency_manager = self.runtime_environment.dependency_manager
        self.dependency_manager.configure(self.runtime_environment)

        self.resource_manager = self.runtime_environment.resource_manager

    def tearDown(self):
        self.mocker.restore()

    @defer.inlineCallbacks
    def test_using_processor(self):
        processed = list()

        registered = list()
        stub_poller = StubSocketPoller(lambda feeder, socket: registered.append(feeder))
        # we don't configure this poller, so it doesn't request any dependencies

        class Provider:
            def add_consumer(self, resource_dependency):
                resource_dependency.on_resource_ready(processed.append)
        provider = Provider()

        # the feeder will request the processor and the socket, we only care about
        # the processor, so it does not matter what the resource given to the
        # zmq.socket.foo resource dependency.
        self.resource_manager.register('pipeline.pipeline_name', provider)
        self.resource_manager.register('zmq.socket.socket_name', provider)

        feeder = providers.ZMQProcessorFeeder(stub_poller, processor_config='pipeline.pipeline_name', socket_name='socket_name')
        feeder.configure(self.runtime_environment)


        self.dependency_manager.resolve_initial_states()

        self.assertIn(feeder, registered) # it should initially register itself, since all deps are ready

        del registered[:] # clear the list

        expected_processed = ['foo', 'bar', 'baz']
        message_batch = ['foo', 'bar', 'baz']
        yield feeder.handle_messages(message_batch)

        # the batons should get processed
        self.assertEquals(processed, expected_processed)
        # and the feeder should re-register itself
        self.assertIn(feeder, registered)

    @defer.inlineCallbacks
    def test_using_processor_with_a_failure(self):
        # swallow the exception caused by our forcibly raised exception.
        mocked_log = self.mocker.replace(log)
        mocked_log.log_traceback(mocker.ARGS, mocker.KWARGS)
        self.mocker.count(1)
        self.mocker.replay()

        processed_ok = list()
        processed_all = list()
        # we set the processor up to throw an exception on the second baton it receives
        def consider_adding_to_processed(baton):
            processed_all.append(baton)
            if len(processed_all) == 2:
                raise Exception('forcibly raised')
            processed_ok.append(baton)

        registered = list()
        stub_poller = StubSocketPoller(lambda feeder, socket: registered.append(feeder))
        # we don't configure this poller, so it doesn't request any dependencies

        class Provider:
            def add_consumer(self, resource_dependency):
                resource_dependency.on_resource_ready(consider_adding_to_processed)
        provider = Provider()

        # the feeder will request the processor and the socket, we only care about
        # the processor, so it does not matter what the resource given to the
        # zmq.socket.foo resource dependency.
        self.resource_manager.register('pipeline.pipeline_name', provider)
        self.resource_manager.register('zmq.socket.socket_name', provider)

        feeder = providers.ZMQProcessorFeeder(stub_poller, processor_config='pipeline.pipeline_name', socket_name='socket_name')
        feeder.configure(self.runtime_environment)

        self.dependency_manager.resolve_initial_states()

        self.assertIn(feeder, registered) # it should initially register itself, since all deps are ready

        del registered[:] # clear the list

        expected_processed = ['foo', 'baz']
        message_batch = ['foo', 'bar', 'baz']
        yield feeder.handle_messages(message_batch)

        # the batons should get processed
        self.assertEquals(processed_ok, expected_processed)
        # and the feeder should re-register itself
        self.assertIn(feeder, registered)
