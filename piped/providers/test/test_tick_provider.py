# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.trial import unittest

from piped import util, processing
from piped.providers import tick_provider


class StubPipelineProvider(object):
    def __init__(self, collector):
        self.process = collector

    def add_consumer(self, resource_dependency):
        resource_dependency.on_resource_ready(self.process)


class TickProviderTest(unittest.TestCase):
    # It's going to complete in a second. If it hasn't, it'll hang
    # until the timeout is reached, so just make it short.
    timeout = 1

    @defer.inlineCallbacks
    def test_tickintervals_created(self):
        provider = tick_provider.TickProvider()
        runtime_environment = processing.RuntimeEnvironment()

        dependency_manager = runtime_environment.dependency_manager
        dependency_manager.configure(runtime_environment)

        configuration_manager = runtime_environment.configuration_manager
        configuration_manager.set('ticks.interval.my_interval',
            dict(
                interval=0, # creates a baton every reactor iteration
                processor='pipeline.pipeline_name'
            )
        )

        ticks = defer.DeferredQueue()

        resource_manager = runtime_environment.resource_manager
        resource_manager.register('pipeline.pipeline_name', StubPipelineProvider(ticks.put))

        provider.configure(runtime_environment)
        provider.startService()

        dependency_manager.resolve_initial_states()

        yield ticks.get()

        provider.stopService()

        # give the tick-interval 1 reactor iteration to shut down
        yield util.wait(0)


    def test_disabled_tickintervals_not_created(self):
        provider = tick_provider.TickProvider()
        runtime_environment = processing.RuntimeEnvironment()

        dependency_manager = runtime_environment.dependency_manager
        dependency_manager.configure(runtime_environment)

        configuration_manager = runtime_environment.configuration_manager
        configuration_manager.set('ticks.interval.my_interval',
            dict(enabled=False, interval=0, processor='pipeline.pipeline_name')
        )
        configuration_manager.set('ticks.interval.another_interval',
            dict(enabled=True, interval=0, processor='pipeline.another_name')
        )

        provider.configure(runtime_environment)

        self.assertEquals(len(provider._tick_intervals), 1)
        self.assertEquals(provider._tick_intervals['another_interval'].processor_dependency_config, dict(provider='pipeline.another_name'))

    def test_tickprovider_globally_disabled(self):
        provider = tick_provider.TickProvider()
        runtime_environment = processing.RuntimeEnvironment()

        dependency_manager = runtime_environment.dependency_manager
        dependency_manager.configure(runtime_environment)

        configuration_manager = runtime_environment.configuration_manager
        configuration_manager.set('ticks.enabled', False)
        configuration_manager.set('ticks.interval.my_interval',
            dict(interval=0, processor='pipeline.pipeline_name')
        )

        provider.configure(runtime_environment)

        self.assertEquals(provider._tick_intervals, dict())

    @defer.inlineCallbacks
    def test_tickintervals_provided(self):
        provider = tick_provider.TickProvider()
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        configuration_manager = runtime_environment.configuration_manager
        configuration_manager.set('ticks.interval.my_interval',
            dict(
                interval=0, # creates a baton every reactor iteration
                processor='pipeline.pipeline_name'
            )
        )

        resource_manager = runtime_environment.resource_manager
        resource_manager.register('pipeline.pipeline_name', StubPipelineProvider(lambda x: x))

        provider.configure(runtime_environment)

        dependency_manager = runtime_environment.dependency_manager
        tick_dependency = dependency_manager.add_dependency(self, dict(provider='ticks.interval.my_interval'))
        dependency_manager.resolve_initial_states()

        tick_interval = yield tick_dependency.wait_for_resource()

        self.assertIsInstance(tick_interval, tick_provider.TickInterval)


class TickIntervalTest(unittest.TestCase):
    # It's going to complete in a second. If it hasn't, it'll hang
    # until the timeout is reached, so just make it short.
    timeout = 1

    @defer.inlineCallbacks
    def test_that_ticks_are_generated(self):
        # ticks every reactor iteration
        source = tick_provider.TickInterval('test_interval', 0, processor='pipeline.a_pipeline_name')

        ticks = defer.DeferredQueue()

        fake_dependencies = util.AttributeDict(wait_for_resource=lambda key: defer.succeed(ticks.put))
        source.dependencies = fake_dependencies
        source.running = True

        d = source.produce_ticks()

        # get 10 ticks, hopefully not spending much more than 10 ms, but this may vary depending
        # on the speed of the machine running the test.
        for i in range(10):
            yield ticks.get()

        source.stopService()
        # wait for the processing to complete
        yield d

    @defer.inlineCallbacks
    def test_waiting_for_completion_generating_new_tick(self):
        # ticks every reactor iteration
        source = tick_provider.TickInterval('test_interval', 0, processor='pipeline.a_pipeline_name')

        collected_ticks = defer.DeferredQueue()

        @defer.inlineCallbacks
        def collector(baton):
            yield util.wait(0.001) # spend at least 1 ms "processing"
            collected_ticks.put(baton)

        fake_dependencies = util.AttributeDict(wait_for_resource=lambda key: defer.succeed(collector))
        source.dependencies = fake_dependencies
        source.running = True

        d = source.produce_ticks()

        yield collected_ticks.get() # wait for the first tick to be provided

        # waiting two reactor iterations:
        yield util.wait(0) # one in order for interval continue processing..
        yield util.wait(0) # .. and one in order let the interval complete its wait(0)
        source.stopService()
        yield d # wait for the interval to stop producing ticks

        # in this situation, one more baton will be created
        self.assertEquals(len(collected_ticks.pending), 1)

    @defer.inlineCallbacks
    def test_restart_without_duplicates_during_sleeping(self):
        # ticks every reactor iteration
        interval = tick_provider.TickInterval('test_interval', 0, processor='pipeline.a_pipeline_name')

        ticks = defer.DeferredQueue()

        def collector(baton):
            ticks.put(baton)

        interval.dependencies = util.AttributeDict(wait_for_resource=lambda key: defer.succeed(collector))
        interval.running = True

        # this immediately produces a tick
        d = interval.produce_ticks()

        # now the producer is sleeping
        interval.stopService()
        # calling startService immediately produces a new tick
        interval.startService()

        # waiting 1 reactor iteration should produce a total of 3 ticks
        yield util.wait(0)
        self.assertEquals(len(ticks.pending), 3)

        interval.stopService()
        # wait for the processing to complete
        yield d

    @defer.inlineCallbacks
    def test_restart_without_duplicates_during_processing(self):
        # ticks every reactor iteration
        interval = tick_provider.TickInterval('test_interval', 0, processor='pipeline.a_pipeline_name')

        ticks = defer.DeferredQueue()

        @defer.inlineCallbacks
        def collector(baton):
            ticks.put(baton)
            yield util.wait(0)

        interval.dependencies = util.AttributeDict(wait_for_resource=lambda key: defer.succeed(collector))
        interval.running = True

        # this immediately produces a tick
        d = interval.produce_ticks()

        yield ticks.get()

        # now the producer is waiting in processing
        interval.stopService()
        # calling startService before the processing is complete should avoid the restart altogether
        interval.startService()

        # waiting 2 reactor iteration should produce a single tick
        yield util.wait(0) # one for the first processing to complete
        yield util.wait(0) # .. and one in order let the interval complete its wait(0)
        self.assertEquals(len(ticks.pending), 1)

        interval.stopService()
        # wait for the processing to complete
        yield d

    @defer.inlineCallbacks
    def test_start_stop_ticking(self):
        # ticks every reactor iteration
        source = tick_provider.TickInterval('test_interval', 0, processor='pipeline.a_pipeline_name', auto_start=False)

        collected_ticks = defer.DeferredQueue()

        fake_dependencies = util.AttributeDict(wait_for_resource=lambda key: defer.succeed(collected_ticks.put))
        source.dependencies = fake_dependencies

        # simply starting the service should not produce any ticks
        source.startService()
        yield util.wait(0)
        self.assertEquals(collected_ticks.pending, list())

        # but explicitly telling it to start ticking should produce a tick every reactor iteration:
        source.start_ticking()
        yield collected_ticks.get() # wait for the first tick to be provided

        self.assertEquals(collected_ticks.pending, list())

        # after we ask it to stop ticking, no more ticks should be produced:
        source.stop_ticking()
        yield util.wait(0)
        self.assertEquals(collected_ticks.pending, list())