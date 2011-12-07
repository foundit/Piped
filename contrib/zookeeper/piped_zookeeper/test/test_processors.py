# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
import mock
import zookeeper

from piped import processing
from twisted.trial import unittest
from piped_zookeeper import processors, providers


class StubZooKeeperProcessor(processors.ZooKeeperProcessor):
    name = None

    def process(self, baton):
        raise NotImplementedError()


class TestZooKeeperProcessor(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

        self.client = mock.Mock()
        self.client_dependency = self.runtime_environment.dependency_manager.as_dependency(self.client)
        self.client_dependency.is_ready = True


class TestZooKeeperProcessorDepending(TestZooKeeperProcessor):

    @defer.inlineCallbacks
    def test_depending_on_zookeeper_client(self):
        provider = providers.ZookeeperClientProvider()
        cm = self.runtime_environment.configuration_manager
        cm.set('zookeeper.clients.test_client', dict(
            servers='localhost:2181'
        ))

        provider.configure(self.runtime_environment)
        processor = StubZooKeeperProcessor(client='test_client')
        processor.configure(self.runtime_environment)

        self.runtime_environment.dependency_manager.resolve_initial_states()
        provider._client_by_name['test_client']._started()

        client = yield processor.client_dependency.wait_for_resource()
        self.assertIsInstance(client, providers.PipedZookeeperClient)


class TestGetChildren(TestZooKeeperProcessor):
    @defer.inlineCallbacks
    def test_getting_children(self):
        processor = processors.GetZooKeeperChildren(client='test', path='/foo/bar')
        processor.client_dependency = self.client_dependency

        self.client.cached_get_children.return_value = ['zip', 'zap']
        baton = yield processor.process(dict())
        self.assertEquals(baton['children'], ['zip', 'zap'])
        self.client.cached_get_children.assert_called_once_with('/foo/bar')

    @defer.inlineCallbacks
    def test_trailing_slash(self):
        processor = processors.GetZooKeeperChildren(client='test', path='/foo/bar/')
        processor.client_dependency = self.client_dependency

        # trailing slashes should be removed
        self.client.cached_get_children.return_value = ['zip', 'zap']
        baton = yield processor.process(dict())
        self.assertEquals(baton['children'], ['zip', 'zap'])
        self.client.cached_get_children.assert_called_once_with('/foo/bar')

        # if the path is a single slash, do not remove it
        self.client.reset_mock()
        processor.path = '/'
        baton = yield processor.process(dict())
        self.assertEquals(baton['children'], ['zip', 'zap'])
        self.client.cached_get_children.assert_called_once_with('/')


class TestGetData(TestZooKeeperProcessor):
    @defer.inlineCallbacks
    def test_getting_data(self):
        processor = processors.GetZooKeeperData(client='test', path='/foo/bar', metadata_output_path='meta')
        processor.client_dependency = self.client_dependency

        self.client.cached_get.return_value = 'test data', dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(data='test data', meta=dict(metadata=True)))
        self.client.cached_get.assert_called_once_with('/foo/bar')


class TestNodeExists(TestZooKeeperProcessor):
    @defer.inlineCallbacks
    def test_node_exists(self):
        processor = processors.ZooKeeperNodeExists(client='test', path='/foo/bar', metadata_output_path='meta')
        processor.client_dependency = self.client_dependency

        self.client.cached_exists.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(exists=True, meta=dict(metadata=True)))
        self.client.cached_exists.assert_called_once_with('/foo/bar')


class TestSetData(TestZooKeeperProcessor):
    @defer.inlineCallbacks
    def test_set_data(self):
        processor = processors.SetZooKeeperData(client='test', path='/foo/bar', data='foo')
        processor.client_dependency = self.client_dependency

        self.client.set.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(set=dict(metadata=True)))
        self.client.set.assert_called_once_with('/foo/bar', 'foo')

    @defer.inlineCallbacks
    def test_creating_intermediary(self):
        processor = processors.SetZooKeeperData(client='test', path='/foo/bar', data='foo', create_intermediary_nodes=True)
        processor.client_dependency = self.client_dependency

        # Pretend that only the root exists:
        self.client.exists.side_effect = lambda path: True if path == '/' else False
        self.client.set.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(set=dict(metadata=True)))
        self.client.set.assert_called_once_with('/foo/bar', 'foo')

        # _ensure should check parent paths up to the root ...
        exists_call_args = [args[0] for args, kwargs in self.client.exists.call_args_list]
        self.assertEquals(exists_call_args, ['/foo/bar', '/foo', '/'])

        # ... and create the missing nodes
        create_call_args = [args[0] for args, kwargs in self.client.create.call_args_list]
        self.assertEquals(create_call_args, ['/foo', '/foo/bar'])


class TestCreateNode(TestZooKeeperProcessor):
    @defer.inlineCallbacks
    def test_create_node(self):
        processor = processors.CreateZooKeeperNode(client='test', path='/foo/bar', data='foo')
        processor.client_dependency = self.client_dependency

        self.client.create.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(create=dict(metadata=True)))
        self.client.create.assert_called_once_with('/foo/bar', 'foo', flags=0)

    @defer.inlineCallbacks
    def test_flags(self):
        processor = processors.CreateZooKeeperNode(client='test', path='/foo/bar', data='foo', flags=['SEQUENCE', 'EPHEMERAL'])
        processor.client_dependency = self.client_dependency

        self.client.create.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(create=dict(metadata=True)))
        self.client.create.assert_called_once_with('/foo/bar', 'foo', flags=zookeeper.SEQUENCE|zookeeper.EPHEMERAL)

    @defer.inlineCallbacks
    def test_flags_as_string(self):
        processor = processors.CreateZooKeeperNode(client='test', path='/foo/bar', data='foo', flags='SEQUENCE')
        processor.client_dependency = self.client_dependency

        self.client.create.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(create=dict(metadata=True)))
        self.client.create.assert_called_once_with('/foo/bar', 'foo', flags=zookeeper.SEQUENCE)

    @defer.inlineCallbacks
    def test_creating_intermediary(self):
        processor = processors.CreateZooKeeperNode(client='test', path='/foo/bar', data='my_data', create_intermediary_nodes=True)
        processor.client_dependency = self.client_dependency

        # Pretend that only the root exists:
        self.client.exists.side_effect = lambda path: True if path == '/' else False
        self.client.create.return_value = dict(metadata=True)
        baton = yield processor.process(dict())
        self.assertEquals(baton, dict(create=dict(metadata=True)))

        # _ensure should check parent paths up to the root ...
        exists_call_args = [args[0] for args, kwargs in self.client.exists.call_args_list]
        self.assertEquals(exists_call_args, ['/foo', '/'])

        # ... and create the missing nodes
        create_call_args = [args for args, kwargs in self.client.create.call_args_list]
        self.assertEquals(create_call_args, [('/foo', ''), ('/foo/bar', 'my_data')])