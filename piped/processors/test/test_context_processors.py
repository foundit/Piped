# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.trial import unittest

from piped import exceptions, processing, util
from piped.processors import context_processors
from piped.providers import context_provider


class ContextFetcherTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()

        self.dependency_manager = self.runtime_environment.dependency_manager
        self.dependency_manager.configure(self.runtime_environment)

        self.provider = context_provider.ContextProvider()

        self.contexts = dict()
        self.runtime_environment.configuration_manager.set('contexts', self.contexts)

        self.processor = context_processors.ContextFetcher('foo')

    def test_fetching_a_context(self):
        self.contexts['foo'] = 42

        self.provider.configure(self.runtime_environment)
        self.processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        baton = self.processor.process(dict())

        self.assertEquals(baton, dict(foo=42))

    def test_fetching_an_unprovided_context(self):
        self.provider.configure(self.runtime_environment)
        self.processor.configure(self.runtime_environment)

        self.assertRaises(exceptions.UnprovidedResourceError, self.dependency_manager.resolve_initial_states)

    def test_replacing_baton(self):
        self.processor = context_processors.ContextFetcher('foo', output_path='')
        self.contexts['foo'] = 42

        self.provider.configure(self.runtime_environment)
        self.processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        baton = self.processor.process(dict())

        self.assertEquals(baton, 42)


class PeristedContextFetcherTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()

        self.dependency_manager = self.runtime_environment.dependency_manager
        self.dependency_manager.configure(self.runtime_environment)

        self.provider = context_provider.PersistedContextProvider()

        self.persisted_contexts = dict(persisted=dict(initial_data=dict()))
        self.runtime_environment.configuration_manager.set('persisted_contexts', self.persisted_contexts)

        self.processor = context_processors.PersistedContextFetcher('persisted')

    @defer.inlineCallbacks
    def test_fetching_a_context(self):
        self.persisted_contexts['persisted']['initial_data'] = dict(foo=42)

        self.provider.configure(self.runtime_environment)
        self.processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        yield self.processor.context_dependency.wait_for_resource()

        baton = self.processor.process(dict())

        self.assertEquals(baton, dict(persisted=dict(foo=42)))

    def test_fetching_an_unprovided_context(self):
        self.persisted_contexts.clear()

        self.provider.configure(self.runtime_environment)
        self.processor.configure(self.runtime_environment)

        self.assertRaises(exceptions.UnprovidedResourceError, self.dependency_manager.resolve_initial_states)

    @defer.inlineCallbacks
    def test_replacing_baton(self):
        self.persisted_contexts['persisted']['initial_data'] = 42

        self.processor = context_processors.PersistedContextFetcher('persisted', output_path='')
        self.provider.configure(self.runtime_environment)
        self.processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        yield self.processor.context_dependency.wait_for_resource()

        baton = self.processor.process(dict())

        self.assertEquals(baton, 42)
