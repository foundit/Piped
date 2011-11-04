# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer, reactor
from twisted.trial import unittest

from piped import util, processing
from piped.providers import system_events_provider


class StubPipelineProvider(object):
    def __init__(self, collector):
        self.processor = collector

    def add_consumer(self, resource_dependency):
        resource_dependency.on_resource_ready(self.processor)


class SystemEventsProviderTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

    @defer.inlineCallbacks
    def test_system_events_triggered(self):
        batons = list()
        stub_pipeline_provider = StubPipelineProvider(batons.append)
        self.runtime_environment.resource_manager.register('pipeline.test_pipeline', stub_pipeline_provider)
        
        provider = system_events_provider.SystemEventsProvider()

        configuration_manager = self.runtime_environment.configuration_manager

        for event_type in 'startup', 'shutdown':
            configuration_manager.set('system-events.%s.name' % event_type, 'pipeline.test_pipeline')

        provider.configure(self.runtime_environment)

        self.runtime_environment.dependency_manager.resolve_initial_states()

        # wait a reactor iteration so the startup event is triggered
        yield util.wait(0)
        self.assertEquals(batons, [dict(event_type='startup')])
        batons[:] = list()

        # trigger the shutdown event, which should give our processor a baton
        reactor.fireSystemEvent('shutdown')
        self.assertEquals(batons, [dict(event_type='shutdown')])