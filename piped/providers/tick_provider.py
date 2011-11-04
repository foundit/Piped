# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import time

from twisted.application import service
from twisted.internet import defer
from zope import interface

from piped import event, log, util, resource


class TickProvider(object, service.MultiService):
    """ Provides tick-batons that are sent to processors at regular intervals.

    Example configuration::

        ticks:
            interval:
                any_name:
                    interval: 120
                    processor: processor_name
                    auto_start: true # if true, starts the interval when the application starts.

    The above will create a :class:`TickInterval` that generates a baton every 120 seconds or
    every time the previous tick baton finished processing, whichever takes the
    longest.

    .. seealso:: :mod:`piped.processors.tick_processors`.
    
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self.on_start = event.Event()
        self.on_pause = event.Event()

        self._tick_intervals = dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.setServiceParent(runtime_environment.application)

        enabled = runtime_environment.get_configuration_value('ticks.enabled', True)
        if not enabled:
            return

        for interval_name, interval_config in runtime_environment.get_configuration_value('ticks.interval', dict()).items():
            if not interval_config.pop('enabled', True):
                continue

            tick_interval = TickInterval(interval_name, **interval_config)
            tick_interval.configure(runtime_environment)
            tick_interval.setServiceParent(self)

            self._tick_intervals[interval_name] = tick_interval

            runtime_environment.resource_manager.register('ticks.interval.%s'%interval_name, provider=self)

    def add_consumer(self, resource_dependency):
        tick, interval, interval_name = resource_dependency.provider.split('.')

        tick_interval = self._tick_intervals[interval_name]
        resource_dependency.on_resource_ready(tick_interval)


class TickInterval(object, service.Service):
    _waiting_for_processor = None
    _sleeping = None

    def __init__(self, interval_name, interval, processor, auto_start=True):
        self.name = interval_name
        self.interval = interval
        self.processor_dependency_config = dict(provider=processor) if isinstance(processor, basestring) else processor

        self._can_start = auto_start

        self._previous_tick = time.time()

    def configure(self, runtime_environment):
        self.dependencies = runtime_environment.create_dependency_map(self, processor=self.processor_dependency_config)

    def start_ticking(self):
        self._can_start = True
        if not self.running:
            self.startService()

    def stop_ticking(self):
        self._can_start = False
        if self.running:
            self.stopService()

    def startService(self):
        if not self._can_start:
            # the user has explicitly disallowed us from starting
            return

        service.Service.startService(self)
        
        if self._waiting_for_processor:
            return

        self.produce_ticks()

    def stopService(self):
        service.Service.stopService(self)
        if self._sleeping:
            self._sleeping.cancel()

    def _create_baton(self):
        tick = time.time()
        delta = tick - self._previous_tick
        baton = dict(interval=self.interval, previous_tick=self._previous_tick, delta=delta, tick=tick)
        self._previous_tick = tick
        return baton

    @defer.inlineCallbacks
    def produce_ticks(self):
        while self.running:
            try:
                self._waiting_for_processor = self.dependencies.wait_for_resource('processor')
                processor = yield self._waiting_for_processor

                baton = self._create_baton()

                self._waiting_for_processor = processor(baton)
                yield self._waiting_for_processor
            except Exception as e:
                log.error()
            finally:
                self._waiting_for_processor = None

            # we might have stopped running while waiting for the processor to finish processing
            if not self.running:
                return

            # the processing might have taken some time, so subtract the time taken from the interval before waiting
            # we set the minimum sleep time to 0 in order to at least wait 1 reactor iteration between every
            # produced baton.
            processing_duration = time.time()-self._previous_tick
            sleep_time = max(self.interval-processing_duration, 0)

            try:
                self._sleeping = util.wait(sleep_time)
                yield self._sleeping
            except defer.CancelledError:
                return
            finally:
                self._sleeping = None
