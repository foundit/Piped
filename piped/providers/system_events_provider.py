# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer, reactor
from zope import interface

from piped import resource


class SystemEventsProvider(object):
    """ Provides batons that are sent into pipelines when system events are triggered.

    Example configuration::

        system-events:
            startup:
                logical_name: pipeline_name
            shutdown:
                logical_name: another_pipeline_name

    .. note:: The pipeline name must not include the ``pipeline.`` prefix.

    """
    interface.classProvides(resource.IResourceProvider)

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

        dependencies = dict()

        for event_name, pipeline_name in runtime_environment.get_configuration_value('system-events.startup', dict()).items():
            dependencies[pipeline_name] = dict(provider='pipeline.%s'%pipeline_name)
            reactor.callLater(0, self._relay_event, pipeline_name=pipeline_name, baton=dict(event_type='startup'))

        for event_name, pipeline_name in runtime_environment.get_configuration_value('system-events.shutdown', dict()).items():
            dependencies[pipeline_name] = dict(provider='pipeline.%s'%pipeline_name)
            reactor.addSystemEventTrigger('before', 'shutdown', self._relay_event, pipeline_name=pipeline_name, baton=dict(event_type='shutdown'))

        if dependencies:
            self.dependencies = runtime_environment.create_dependency_map(self, **dependencies)

    @defer.inlineCallbacks
    def _relay_event(self, pipeline_name, baton):
        pipeline = yield self.dependencies.wait_for_resource(pipeline_name)

        yield pipeline.process(baton)