# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json

from twisted.internet import defer, reactor, threads
from zope import interface

from piped import exceptions, log, resource, util

try:
    import cPickle as pickle
except ImportError:
    import pickle


class NoSuchContextError(exceptions.PipedError):
    pass


class ContextProvider(object):
    """ Provides shared data as resources.

    Example configuration::

        contexts:
            my_context:
                foo: bar
            another_context: this is a string

    The above contexts would be made available as ``context.my_context` and
    ``context.another_context``.

    """
    interface.classProvides(resource.IResourceProvider)
    contexts = dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager
        self.contexts = runtime_environment.get_configuration_value('contexts', dict())

        # State that we can provide all configured contexts.
        resource_manager = runtime_environment.resource_manager
        for context_name, context in self.contexts.items():
            resource_manager.register('context.%s' % context_name, provider=self)

    def add_consumer(self, resource_dependency):
        context_type, context_name = resource_dependency.provider.split('.')

        self._fail_if_context_is_invalid(context_type, context_name)

        resource_dependency.on_resource_ready(self.contexts[context_name])
        self.runtime_environment.dependency_manager.add_dependency(resource_dependency, self)

    def _fail_if_context_is_invalid(self, context_type, context_name):
        if context_name not in self.contexts:
            raise NoSuchContextError('%s.%s' % (context_type, context_name))


class PersistedContextProvider(object):
    """ Provides shared data as resources, which is persisted when the process is stopped.

    Example configuration::

        persisted_contexts:
            some_context:
                kind: json
                file: some_context.json
                initial_data: {}


    The above contexts would be made available as
    ``persisted_context.some_context``. When the service starts, it
    reads ``some_context.json``, if it exists, and its contents are
    provided. If the file does not exist or the contents cannot be
    parsed, the data in `initial_data` is provided instead.
    """
    interface.classProvides(resource.IResourceProvider)
    persisted_contexts = dict()

    def __init__(self):
        self._loading_deferreds = dict()
        self._contexts_to_persist = dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager
        self.persisted_contexts = runtime_environment.get_configuration_value('persisted_contexts', dict())

        # State that we can provide all configured contexts.
        resource_manager = runtime_environment.resource_manager
        for context_name, context in self.persisted_contexts.items():
            resource_manager.register('persisted_context.%s' % context_name, provider=self)

        # Hook up the shutdown-event
        if not util.in_unittest():
            reactor.addSystemEventTrigger('before', 'shutdown', self.persist_contexts)

    def add_consumer(self, resource_dependency):
        context_type, context_name = resource_dependency.provider.split('.')

        self._fail_if_context_is_invalid(context_type, context_name)

        if context_name in self._contexts_to_persist:
            # It's already loaded, so it's immediately available.
            resource_dependency.on_resource_ready(self._contexts_to_persist[context_name])
        else:
            # Load the data
            d = self._provide_persisted_context(context_name)
            # ... and state ready when it's loaded.
            d.addCallback(lambda _: resource_dependency.on_resource_ready(self._contexts_to_persist[context_name]))

        self.runtime_environment.dependency_manager.add_dependency(resource_dependency, self)

    def _fail_if_context_is_invalid(self, context_type, context_name):
        if context_name not in self.persisted_contexts:
            raise NoSuchContextError('%s.%s' % (context_type, context_name))

        kind = self.persisted_contexts[context_name].get('kind', 'json')
        if kind not in ('json', 'pickle'):
            raise ValueError('Persisted context "%s" has an invalid kind: "%s"' % (context_name, kind))

    @defer.inlineCallbacks
    def _provide_persisted_context(self, context_name):
        if context_name in self._loading_deferreds:
            # We're already loading
            d = self._loading_deferreds[context_name]
        else:
            # Load the file in a thread.
            d = self._loading_deferreds[context_name] = threads.deferToThread(self._load_persisted_context, context_name)

        # Wait until we're done loading, and then return the resulting context.
        context = yield d
        self._loading_deferreds.pop(context_name, None)
        defer.returnValue(context)

    def _load_persisted_context(self, context_name):
        kind = self.persisted_contexts[context_name].get('kind', 'json')
        file_path = self._get_filepath_for_persisted_context(context_name)

        # Set the initial/fallback data.
        context = self.persisted_contexts[context_name].get('initial_data', dict())
        if not file_path.exists():
            self._contexts_to_persist[context_name] = context
            return context

        try:
            assert kind in ('json', 'pickle')
            if kind == 'json':
                context = json.load(file_path.open())
            else:
                context = pickle.load(file_path.open())

            reactor.callFromThread(log.info, 'Loaded context "%s" from "%s"' % (context_name, file_path.path))
        except Exception, e:
            reactor.callFromThread(log.error, 'Could not load persisted context "%s": %s' % (context_name, e.args[0]))
            # Fallbacking to the initial_data.

        self._contexts_to_persist[context_name] = context
        return context

    def _get_filepath_for_persisted_context(self, context_name):
        kind = self.persisted_contexts[context_name].get('kind', 'json')
        file = self.persisted_contexts[context_name].get('file', '%s.%s' % (context_name, kind))
        return util.ensure_filepath(util.expand_filepath(file))

    def persist_contexts(self):
        ds = list()
        for context_name, context in self._contexts_to_persist.items():
            ds.append(self._persist_context(context_name, context))
        return defer.DeferredList(ds)

    @defer.inlineCallbacks
    def _persist_context(self, context_name, context):
        kind = self.persisted_contexts[context_name].get('kind', 'json')
        file_path = self._get_filepath_for_persisted_context(context_name)

        assert kind in ('json', 'pickle')
        try:
            if kind == 'json':
                yield threads.deferToThread(json.dump, context, file_path.open('w'))
            else:
                yield threads.deferToThread(pickle.dump, context, file_path.open('w'))
        except Exception, e:
            log.error('Could not persist context "%s": %s' % (context_name, e.args[0]))
            raise
