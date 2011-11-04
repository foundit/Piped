# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer, reactor
from zope import interface

from piped import resource


class SystemEventsProvider(object):
    """ Provides batons that are sent to processors when system events are triggered.

    Example configuration::

        system-events:
            startup:
                logical_name: processor_name
            shutdown:
                logical_name: another_processor_name

    """
    interface.classProvides(resource.IResourceProvider)

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

        dependencies = dict()

        for event_name, processor in runtime_environment.get_configuration_value('system-events.startup', dict()).items():
            dependencies[processor] = dict(provider=processor) if isinstance(processor, basestring) else processor
            reactor.callLater(0, self._relay_event, processor_name=processor, baton=dict(event_type='startup'))

        for event_name, processor in runtime_environment.get_configuration_value('system-events.shutdown', dict()).items():
            dependencies[processor] = dict(provider=processor) if isinstance(processor, basestring) else processor
            reactor.addSystemEventTrigger('before', 'shutdown', self._relay_event, processor_name=processor, baton=dict(event_type='shutdown'))

        if dependencies:
            self.dependencies = runtime_environment.create_dependency_map(self, **dependencies)

    @defer.inlineCallbacks
    def _relay_event(self, processor_name, baton):
        processor = yield self.dependencies.wait_for_resource(processor_name)
        yield processor(baton)