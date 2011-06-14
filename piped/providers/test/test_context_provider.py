# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
import json
import pickle
import tempfile
import os

from twisted.internet import defer
from twisted.python import filepath
from twisted.trial import unittest

from piped import processing, dependencies
from piped.providers import context_provider


class ContextProviderTest(unittest.TestCase):
    timeout = 3

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()
        self.resource_manager = self.runtime_environment.resource_manager
        self.data_dir = filepath.FilePath(__file__).sibling('data')

    def test_getting_a_context_with_an_invalid_name(self):
        cp = context_provider.ContextProvider()
        cp.configure(self.runtime_environment)
        resource_dependency = dependencies.ResourceDependency(provider='context.foo')
        self.assertRaises(context_provider.NoSuchContextError, cp.add_consumer, resource_dependency)

    def test_that_contexts_are_provided(self):
        cp = context_provider.ContextProvider()
        contexts = dict(
            foo = dict(),
            bar = dict()
        )
        self.runtime_environment.configuration_manager.set('contexts', contexts)

        cp.configure(self.runtime_environment)

        expected_registered = {
            'context.bar': cp,
            'context.foo': cp
        }

        resource_manager = self.runtime_environment.resource_manager
        self.assertEquals(resource_manager.get_registered_resources(), expected_registered)

    def test_no_contexts(self):
        cp = context_provider.ContextProvider()
        self.runtime_environment.configuration_manager.set('contexts', dict())

        cp.configure(self.runtime_environment)

        # no contexts should be provided
        self.assertEquals(self.runtime_environment.resource_manager.get_registered_resources(), dict())

    def test_contexts_of_the_same_name_are_reused(self):
        self.runtime_environment.configuration_manager.set('contexts', dict(foo=dict()))
        cp = context_provider.ContextProvider()
        cp.configure(self.runtime_environment)

        # Request the same context twice.
        resource_dependency = dependencies.ResourceDependency(provider='context.foo')
        same_resource_dependency = dependencies.ResourceDependency(provider='context.foo')

        # The two resource configuration-dicts now have on_available-events.
        contexts = []

        resource_dependency.on_resource_ready += lambda resource: contexts.append(resource)
        same_resource_dependency.on_resource_ready += lambda resource: contexts.append(resource)

        cp.add_consumer(resource_dependency)
        cp.add_consumer(same_resource_dependency)

        # We should then have two contexts
        self.assertEquals(len(contexts), 2)
        # And they must be the same object.
        self.assertTrue(contexts[0] is contexts[1])

    def test_defaults_are_available(self):
        contexts = dict(foo=dict(bar=[1,2,3]))
        self.runtime_environment.configuration_manager.set('contexts', contexts)
        cp = context_provider.ContextProvider()
        cp.configure(self.runtime_environment)

        provided = []
        resource_dependency = dependencies.ResourceDependency(provider='context.foo')
        resource_dependency.on_resource_ready += lambda resource: provided.append(resource)

        cp.add_consumer(resource_dependency)

        # assert that a context has been provided
        self.assertEquals(len(provided), 1)
        # and that it contains the expected contents
        self.assertTrue(provided[0] is contexts['foo'])


class PersistedContextProviderTest(unittest.TestCase):
    timeout = 3

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()
        self.resource_manager = self.runtime_environment.resource_manager
        self.data_dir = filepath.FilePath(__file__).sibling('data')

    @defer.inlineCallbacks
    def test_loading_persisted_context_with_defaults(self):
        file = self.data_dir.child('persisted_json.json').path
        persisted_contexts = dict(persisted_json=dict(file=file))
        self.runtime_environment.configuration_manager.set('persisted_contexts', persisted_contexts)

        cp = context_provider.PersistedContextProvider()
        cp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        resource_dependency = dm.add_dependency(self, dict(provider='persisted_context.persisted_json'))
        dm.resolve_initial_states()

        resource = yield resource_dependency.wait_for_resource()
        self.assertEquals(resource, {"a": 42, "foo": "bar"})

    @defer.inlineCallbacks
    def test_loading_persisted_pickle(self):
        file = self.data_dir.child('persisted_pickle.pickle').path
        persisted_contexts = dict(persisted_pickle=dict(file=file, kind='pickle'))
        self.runtime_environment.configuration_manager.set('persisted_contexts', persisted_contexts)

        cp = context_provider.PersistedContextProvider()
        cp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        resource_dependency = dm.add_dependency(self, dict(provider='persisted_context.persisted_pickle'))
        dm.resolve_initial_states()

        resource = yield resource_dependency.wait_for_resource()
        expected_context = dict(foo='bar', tuple=(1, 2), list=[1,2,3], some_date=datetime.date(2011, 1, 1))
        self.assertEquals(resource, expected_context)

    @defer.inlineCallbacks
    def test_multiple_dependencies_on_the_same_persisted_context(self):
        file = self.data_dir.child('persisted_pickle.pickle').path
        persisted_contexts = dict(persisted_pickle=dict(file=file, kind='pickle'))
        self.runtime_environment.configuration_manager.set('persisted_contexts', persisted_contexts)

        cp = context_provider.PersistedContextProvider()
        cp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        resource_dependency1 = dm.add_dependency(self, dict(provider='persisted_context.persisted_pickle'))
        resource_dependency2 = dm.add_dependency(self, dict(provider='persisted_context.persisted_pickle'))
        dm.resolve_initial_states()

        resource1 = yield resource_dependency1.wait_for_resource()
        resource2 = yield resource_dependency2.wait_for_resource()
        
        expected_context = dict(foo='bar', tuple=(1, 2), list=[1,2,3], some_date=datetime.date(2011, 1, 1))
        self.assertEquals(resource1, expected_context)
        self.assertTrue(resource1 is resource2)

    @defer.inlineCallbacks
    def test_persisting_json_context(self):
        file = self.data_dir.child('persisted_json.json').path
        persisted_contexts = dict(persisted_json=dict(file=file))
        self.runtime_environment.configuration_manager.set('persisted_contexts', persisted_contexts)

        cp = context_provider.PersistedContextProvider()
        cp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        resource_dependency = dm.add_dependency(self, dict(provider='persisted_context.persisted_json'))
        dm.resolve_initial_states()

        resource = yield resource_dependency.wait_for_resource()
        resource['something'] = 'something something'

        tmp_file_path = tempfile.NamedTemporaryFile('r').name
        cp.persisted_contexts['persisted_json']['file'] = tmp_file_path

        try:
            yield cp.persist_contexts()
            self.assertEqual(json.load(open(tmp_file_path)), {"a": 42, "foo": "bar", "something": "something something"})
        finally:
            os.remove(tmp_file_path)

    @defer.inlineCallbacks
    def test_persisting_pickle_context(self):
        file = self.data_dir.child('persisted_pickle.pickle').path
        persisted_contexts = dict(persisted_pickle=dict(file=file, kind='pickle'))
        self.runtime_environment.configuration_manager.set('persisted_contexts', persisted_contexts)

        cp = context_provider.PersistedContextProvider()
        cp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        resource_dependency = dm.add_dependency(self, dict(provider='persisted_context.persisted_pickle'))
        dm.resolve_initial_states()

        resource = yield resource_dependency.wait_for_resource()
        resource['something'] = 'something something'

        tmp_file_path = tempfile.NamedTemporaryFile('r').name
        cp.persisted_contexts['persisted_pickle']['file'] = tmp_file_path

        try:
            yield cp.persist_contexts()
            expected_context = dict(foo='bar', tuple=(1, 2), list=[1,2,3], some_date=datetime.date(2011, 1, 1),
                                    something='something something')
            self.assertEqual(pickle.load(open(tmp_file_path)), expected_context)
        finally:
            os.remove(tmp_file_path)

    @defer.inlineCallbacks
    def test_loading_context_with_nonexisting_file(self):
        file = self.data_dir.child('nosuchfile.json').path
        persisted_contexts = dict(foo=dict(file=file, initial_data=dict(foo='bar')))
        self.runtime_environment.configuration_manager.set('persisted_contexts', persisted_contexts)

        cp = context_provider.PersistedContextProvider()
        cp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        resource_dependency = dm.add_dependency(self, dict(provider='persisted_context.foo'))
        dm.resolve_initial_states()

        resource = yield resource_dependency.wait_for_resource()
        self.assertEqual(resource, dict(foo='bar'))
