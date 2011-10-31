# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer

from piped import exceptions, processing
from twisted.trial import unittest
from piped.contrib.zookeeper import providers


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

        dependency = dm.add_dependency(self, dict(provider='zookeeper.client.test_client'))
        dm.resolve_initial_states()

        # set the client to started, which should provide the client as a resource
        internal_client = provider._client_by_name['test_client']
        internal_client._started()

        client = dependency.get_resource()
        self.assertIsInstance(client, providers.PipedZookeeperClient)


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

    def test_pipelines_depended_on(self):
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

        # all the events should have a pipeline
        for event in events.keys():
            pipeline_name = 'pipeline.{0}'.format(event)
            self.assertIn(pipeline_name, dependency_by_provider_name)

            dependency_by_provider_name.pop(pipeline_name)

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
