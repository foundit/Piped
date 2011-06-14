# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.trial import unittest

from piped.processors import pipeline_processors


class StubPipelineResource(object):
    def __init__(self, processor, waiter=None):
        self.processor = processor
        self.waiter = waiter
        if not waiter:
            self.waiter = lambda: self
    def process(self, baton):
        return self.processor(baton)
    def wait_for_resource(self):
        return self.waiter()


class PipelineRunnerTest(unittest.TestCase):
    @defer.inlineCallbacks
    def test_simple_processing(self):
        processed = list()
        def list_appender(baton):
            processed.append(baton)
            return [baton]

        pipeline = StubPipelineResource(list_appender)
        processor = pipeline_processors.PipelineRunner('foo')
        processor.pipeline_dependency = pipeline

        result = yield processor.process('a baton')
        self.assertEquals(processed, ['a baton'])
        self.assertEquals(result, 'a baton')

    @defer.inlineCallbacks
    def test_getting_only_last_result(self):
        processed = list()
        def list_appender(baton):
            processed.append(baton)
            return ['first', 'second', 'last']

        pipeline = StubPipelineResource(list_appender)
        processor = pipeline_processors.PipelineRunner('foo')
        processor.pipeline_dependency = pipeline

        result = yield processor.process('a baton')
        self.assertEquals(result, 'last')

    @defer.inlineCallbacks
    def test_getting_all_results(self):
        processed = list()
        def list_appender(baton):
            processed.append(baton)
            return ['first', 'second', 'last']

        pipeline = StubPipelineResource(list_appender)
        processor = pipeline_processors.PipelineRunner('foo', only_last_result=False)
        processor.pipeline_dependency = pipeline

        result = yield processor.process('a baton')
        self.assertEquals(result, ['first', 'second', 'last'])

    @defer.inlineCallbacks
    def test_waiting_for_pipeline(self):
        processed = list()
        waiting = list()
        def list_appender(baton):
            processed.append(baton)
            return [baton]

        def waiter():
            d = defer.Deferred()
            waiting.append(d)
            return d

        pipeline = StubPipelineResource(list_appender, waiter)
        processor = pipeline_processors.PipelineRunner('foo')
        processor.pipeline_dependency = pipeline

        d_process = processor.process('a baton')

        # the processor should be waiting for its pipeline
        self.assertEquals(processed, list())
        self.assertEquals(len(waiting), 1)

        # when the pipeline becomes available, the baton is processed
        waiting[0].callback(pipeline)
        yield d_process
        self.assertEquals(processed, ['a baton'])
