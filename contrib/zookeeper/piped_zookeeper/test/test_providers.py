# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import mock
import zookeeper
from mock import patch
from txzookeeper import client as txclient
from twisted.internet import defer

from piped import exceptions, processing, dependencies
from twisted.trial import unittest
from piped_zookeeper import providers


class TestClientProvider(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

    def test_client_provided(self):
        dm = self.runtime_environment.dependency_manager
        cm = self.runtime_environment.configuration_manager
        cm.set('zookeeper.clients.test_client',
            dict(
                servers='localhost:2181/foo,localhost:2182/bar',
            )
        )

        provider = providers.ZookeeperClientProvider()
        provider.configure(self.runtime_environment)
        # no events are defined, so the client have not been created yet
        self.assertEquals(provider._client_by_name, dict())

        dependency = dm.add_dependency(self, dict(provider='zookeeper.client.test_client'))
        dm.resolve_initial_states()

        # set the client to started, which should provide the client as a resource
        internal_client = provider._client_by_name['test_client']
        internal_client._started()

        client = dependency.get_resource()
        self.assertIsInstance(client, providers.PipedZookeeperClient)

        another_client_dependency = dm.add_dependency(self, dict(provider='zookeeper.client.test_client'))
        dm.resolve_initial_states()

        # the different dependencies should get the same client instance:
        self.assertNotEquals(another_client_dependency, dependency)
        self.assertIdentical(client, another_client_dependency.get_resource())

    def test_client_created_automatically_if_events(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('zookeeper.clients.test_client',
            dict(
                servers = 'localhost:2181/foo,localhost:2182/bar',
                events = dict(
                    starting = 'processor_name'
                )
            )
        )

        provider = providers.ZookeeperClientProvider()
        provider.configure(self.runtime_environment)
        # an event is defined, so the client should have been created
        self.assertNotEquals(provider._client_by_name, dict())


class TestClient(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

    def test_invalid_event(self):
        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar',
            events=dict(foo='bar')
        )

        self.assertRaises(exceptions.ConfigurationError, client.configure, self.runtime_environment)

    def test_processors_depended_on(self):
        events = dict(
            zip(providers.PipedZookeeperClient.possible_events, providers.PipedZookeeperClient.possible_events)
        )
        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar',
            events=events
        )

        client.configure(self.runtime_environment)

        dependencies = self.runtime_environment.dependency_manager.get_dependencies_of(client.dependencies)

        dependency_by_provider_name = dict()
        for dependency in dependencies:
            dependency_by_provider_name[dependency.provider] = dependency

        # all the events should have a processor
        for event in events.keys():
            self.assertIn(event, dependency_by_provider_name)
            dependency_by_provider_name.pop(event)

        self.assertEquals(dependency_by_provider_name, dict())

    def _create_cacheable(self):
        pending = list()
        def call(**kwargs):
            d = defer.Deferred()
            watcher = defer.Deferred()
            pending.append((d, kwargs, watcher))
            return d, watcher

        return pending, call

    def test_caching_two_calls_with_same_arguments(self):
        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar'
        )

        pending, cacheable = self._create_cacheable()

        cached_call = client._cached(cacheable)

        # two calls with the same arguments should only result in 1 call to the cached function
        foo = cached_call(foo=True)
        foo_2 = cached_call(foo=True)
        self.assertEquals(len(pending), 1)

        pending.pop()[0].callback('test result')
        self.assertEquals(foo.result, 'test result')
        self.assertEquals(foo_2.result, 'test result')

        # since it callbacked, we should have this value in the cache now
        foo_3 = cached_call(foo=True)
        self.assertEquals(len(pending), 0)

        self.assertEquals(foo_3.result, 'test result')

    def test_exceptions_should_not_be_cached(self):
        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar'
        )

        pending, cacheable = self._create_cacheable()

        cached_call = client._cached(cacheable)
        foo = cached_call(foo=True)
        foo_2 = cached_call(foo=True)
        self.assertEquals(len(pending), 1)

        # both foo and foo_2 should be errbacked
        exception = Exception('42')
        pending.pop()[0].errback(exception)

        foo.addCallback(self.fail)
        foo_2.addCallback(self.fail)
        
        foo.addErrback(lambda reason: self.assertEquals(reason.value, exception))
        foo_2.addErrback(lambda reason: self.assertEquals(reason.value, exception))

        self.assertEquals(len(pending), 0)
        # since it errbacked, we shouldn't have cached this value
        foo_3 = cached_call(foo=True)
        self.assertEquals(len(pending), 1)

    def test_cache_cleared_when_watch_fired(self):
        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar'
        )

        pending, cacheable = self._create_cacheable()
        cached_call = client._cached(cacheable)

        foo = cached_call(foo=True)
        self.assertEquals(len(pending), 1)

        d, kwargs, watcher = pending.pop()
        d.callback(42)
        self.assertEquals(foo.result, 42)

        # the value should be cached:
        foo_2 = cached_call(foo=True)
        self.assertEquals(len(pending), 0)
        self.assertEquals(foo_2.result, 42)

        # callbacking the watcher should clear the cache:
        watcher.callback(None)
        foo_3 = cached_call(foo=True)
        self.assertEquals(len(pending), 1)

    def test_cache_cleared_when_client_disconnects(self):
        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar'
        )

        pending, cacheable = self._create_cacheable()
        cached_call = client._cached(cacheable)

        foo = cached_call(foo=True)
        self.assertEquals(len(pending), 1)

        d, kwargs, watcher = pending.pop()
        d.callback(42)
        self.assertEquals(foo.result, 42)

        # the value should be cached:
        foo_2 = cached_call(foo=True)
        self.assertEquals(len(pending), 0)
        self.assertEquals(foo_2.result, 42)

        # a client disconnect should clear the cache
        client.on_disconnected('test disconnect')
        foo_3 = cached_call(foo=True)
        self.assertEquals(len(pending), 1)
        
    def test_events(self):
        events = dict(
            zip(providers.PipedZookeeperClient.possible_events, providers.PipedZookeeperClient.possible_events)
        )

        client = providers.PipedZookeeperClient(
            servers='localhost:2181/foo,localhost:2182/bar',
            events = events
        )
        client.configure(self.runtime_environment)

        with patch.object(client, 'connect') as mocked_connect:
            connecting = defer.Deferred()
            mocked_connect.return_value = connecting

            with patch.object(client, 'dependencies') as mocked_dependencies:
                # starting the service should result in the 'starting event'
                client.startService()
                mocked_dependencies.wait_for_resource.assert_called_with('starting')

                # establishing a connection should result in the 'connected' event:
                connecting.callback(True)
                mocked_dependencies.wait_for_resource.assert_called_with('connected')

                # suddenly it is reconnecting
                connecting_event = txclient.ClientEvent(type=zookeeper.SESSION_EVENT, connection_state=zookeeper.CONNECTING_STATE, path=None)
                client._watch_connection(client, connecting_event)
                mocked_dependencies.wait_for_resource.assert_called_with('reconnecting')

                # and we're connected again:
                connected_event = txclient.ClientEvent(type=zookeeper.SESSION_EVENT, connection_state=zookeeper.CONNECTED_STATE, path=None)
                client._watch_connection(client, connected_event)
                mocked_dependencies.wait_for_resource.assert_called_with('reconnected')

                # stopping the service should result in the stopping event:
                client.stopService()
                mocked_dependencies.wait_for_resource.assert_called_with('stopping')

                # start the service again..
                client.startService()

                # reset the mock in since we're about to capture a series of events:
                mocked_dependencies.reset_mock()

                expired_event = txclient.ClientEvent(type=zookeeper.SESSION_EVENT, connection_state=zookeeper.EXPIRED_SESSION_STATE, path=None)
                client._watch_connection(client, expired_event)

                # the expired event currently causes a full disconnect and reconnect in order to ensure we get a new session:
                fired_events = [args[0] for args, kwargs in mocked_dependencies.wait_for_resource.call_args_list]
                self.assertEquals(fired_events, ['expired', 'stopping', 'starting', 'connected'])

                # auth errors should be logged:
                with patch.object(providers.log, 'warn') as mocked_warn:
                    auth_event = txclient.ClientEvent(type=zookeeper.SESSION_EVENT, connection_state=zookeeper.AUTH_FAILED_STATE, path=None)
                    client._watch_connection(client, auth_event)

                    self.assertEquals(mocked_warn.call_count, 1)
