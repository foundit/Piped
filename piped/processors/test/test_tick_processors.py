# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.application import service
from twisted.internet import defer
from twisted.trial import unittest

from piped import processing, util
from piped.processors import tick_processors
from piped.providers import tick_provider


class TickProcessorTest(unittest.TestCase):
    timeout = 1

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.application = service.MultiService()
        self.runtime_environment.application.startService()
        
        self.runtime_environment.resource_manager.register('pipeline.test_pipeline', self)

        self.dependency_manager = self.runtime_environment.dependency_manager
        self.dependency_manager.configure(self.runtime_environment)

        self.provider = tick_provider.TickProvider()
        self.provider.setServiceParent(self.runtime_environment.application)

        self.intervals = dict()
        self.runtime_environment.configuration_manager.set('ticks.interval', self.intervals)

        self.start_processor = tick_processors.StartInterval('test_interval')
        self.stop_processor = tick_processors.StopInterval('test_interval')

        self.ticks = defer.DeferredQueue()

    def tearDown(self):
        self.runtime_environment.application.stopService()

    def add_consumer(self, resource_dependency):
        resource_dependency.on_resource_ready(self.process)

    def process(self, baton):
        return self.ticks.put(baton)

    @defer.inlineCallbacks
    def test_starting_an_interval(self):
        self.intervals['test_interval'] = dict(interval=0, auto_start=False, processor='pipeline.test_pipeline')

        self.provider.configure(self.runtime_environment)
        self.start_processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        # since the interval isn't started, no ticks should have been created
        self.assertEquals(self.ticks.pending, list())

        # running the start processor should start the ticking:
        self.start_processor.process(dict())
        yield self.ticks.get()
        self.assertEquals(self.ticks.pending, list())

    @defer.inlineCallbacks
    def test_stopping_an_interval(self):
        self.intervals['test_interval'] = dict(interval=0, processor='pipeline.test_pipeline')

        self.provider.configure(self.runtime_environment)
        self.stop_processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        # the interval autostarts and produces a tick
        self.assertEquals(len(self.ticks.pending), 1)
        yield self.ticks.get()

        # running the stop processor should stop the ticking:
        yield self.stop_processor.process(dict())
        yield util.wait(0)
        self.assertEquals(self.ticks.pending, list())

    @defer.inlineCallbacks
    def test_restarting_an_interval(self):
        self.intervals['test_interval'] = dict(interval=0, auto_start=False, processor='pipeline.test_pipeline')

        self.provider.configure(self.runtime_environment)
        self.start_processor.configure(self.runtime_environment)
        self.stop_processor.configure(self.runtime_environment)
        self.dependency_manager.resolve_initial_states()

        # since the interval isn't started, no ticks should have been created
        self.assertEquals(self.ticks.pending, list())

        # running the start processor should start the ticking:
        self.start_processor.process(dict())
        yield self.ticks.get()
        self.assertEquals(self.ticks.pending, list())

        # running the stop processor should stop the ticking:
        yield self.stop_processor.process(dict())
        yield util.wait(0)
        self.assertEquals(self.ticks.pending, list())

        # running the start processor again should restart the ticking:
        self.start_processor.process(dict())
        yield self.ticks.get()
        self.assertEquals(self.ticks.pending, list())