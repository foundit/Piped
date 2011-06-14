# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Processors that deal with tick intervals. """
from twisted.internet import defer
from zope import interface

from piped import processing
from piped.processors import base


class IntervalProcessor(base.Processor):

    def __init__(self, interval, **kw):
        """
        :param interval: The name of the interval. See :class:`~piped.providers.tick_provider.TickProvider`.
        """
        super(IntervalProcessor, self).__init__(**kw)

        self.interval_name = interval

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.interval_dependency = dm.add_dependency(self, dict(provider='ticks.interval.%s'%self.interval_name))

    @defer.inlineCallbacks
    def process(self, baton):
        interval = yield self.interval_dependency.wait_for_resource()

        yield self.process_interval(interval, baton)

        defer.returnValue(baton)

    def process_interval(self, interval, baton):
        raise NotImplemented('This method must be overridden by subclasses.')


class StartInterval(IntervalProcessor):
    """ Starts a tick interval. """
    name = 'start-tick-interval'
    interface.classProvides(processing.IProcessor)

    def process_interval(self, interval, baton):
        interval.start_ticking()


class StopInterval(IntervalProcessor):
    """ Stops a tick interval. """
    name = 'stop-tick-interval'
    interface.classProvides(processing.IProcessor)

    def process_interval(self, interval, baton):
        interval.stop_ticking()
