# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from zope import interface

from piped import util, processing
from piped.processors import base


class ContextFetcher(base.Processor):
    """ Fetches a shared context.

    If output_path is None, it defaults to the same as the context name.
    """
    interface.classProvides(processing.IProcessor)
    name = 'fetch-context'

    def __init__(self, context, output_path=Ellipsis, **kw):
        super(ContextFetcher, self).__init__(**kw)
        self.context = context
        if output_path is Ellipsis:
            output_path = context
        self.output_path = output_path

    def configure(self, runtime_environment):
        self.context_dependency = runtime_environment.dependency_manager.add_dependency(self, dict(provider='context.%s' % self.context))

    def process(self, baton):
        context = self.context_dependency.get_resource()
        if self.output_path == '':
            return context
        util.dict_set_path(baton, self.output_path, context)
        return baton


class PersistedContextFetcher(ContextFetcher):
    interface.classProvides(processing.IProcessor)
    name = 'fetch-persisted-context'

    def configure(self, runtime_environment):
        self.context_dependency = runtime_environment.dependency_manager.add_dependency(self, dict(provider='persisted_context.%s' % self.context))
