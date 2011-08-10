# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.trial import unittest

from piped import processing, exceptions
from piped.processors import pipeline_processors
from piped.providers import pipeline_provider


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

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

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

    def test_invalid_processor_configuration(self):
        self.assertRaises(exceptions.ConfigurationError, pipeline_processors.PipelineRunner, pipeline_path='pipeline', pipeline='pipeline')
        # the trace is not allowed to replace the baton:
        self.assertRaises(exceptions.ConfigurationError, pipeline_processors.PipelineRunner, pipeline='pipeline', trace_path='')

    @defer.inlineCallbacks
    def test_simple_static_pipeline_dependency(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pipelines.test', [{'run-pipeline':dict(pipeline='foo')}])
        cm.set('pipelines.foo', [{'set-value':dict(path='foo', value=42)}])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        test_pipeline = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()

        results = yield pipeline.process(dict())

        self.assertEquals(results, [dict(foo=42)])
        self.assertEquals(dm.get_dependencies_of(pipeline[0]), [pipeline[0].pipeline_dependency])

    @defer.inlineCallbacks
    def test_simple_dynamic_pipeline_dependency(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pipelines.test', [{'run-pipeline':dict(pipeline_path='pipeline')}])
        cm.set('pipelines.foo', [{'set-value':dict(path='foo', value=42)}])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        test_pipeline = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()

        results = yield pipeline.process(dict(pipeline='foo'))

        self.assertEquals(results, [dict(foo=42, pipeline='foo')])
        self.assertEquals(dm.get_dependencies_of(pipeline[0]), list())

    @defer.inlineCallbacks
    def test_dynamic_pipeline_nonexistent_pipeline(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pipelines.test', [{'run-pipeline':dict(pipeline_path='pipeline')}])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        test_pipeline = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()

        try:
            yield pipeline.process(dict(pipeline='foo'))
            self.fail('Expected the pipeline "foo" to not be provided.')
        except exceptions.UnprovidedResourceError as e:
            pass

        self.assertEquals(dm.get_dependencies_of(pipeline[0]), list())

    @defer.inlineCallbacks
    def test_dynamic_pipeline_nonexistent_processor(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pipelines.test', [{'run-pipeline':dict(pipeline_path='pipeline')}])
        cm.set('pipelines.foo', ['no-such-processor'])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        test_pipeline = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()

        try:
            yield pipeline.process(dict(pipeline='foo'))
            self.fail('Expected the pipeline "foo" to contain a processor that does not exist.')
        except exceptions.ConfigurationError as e:
            self.assertIn('invalid plugin name: "no-such-processor"', e.args[0])

        self.assertEquals(dm.get_dependencies_of(pipeline[0]), list())

    @defer.inlineCallbacks
    def test_static_with_tracing(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pipelines.test', [{'run-pipeline':dict(pipeline='foo', trace_path='trace')}])
        cm.set('pipelines.foo', [{'set-value':dict(path='foo', value=42)}])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        test_pipeline = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()

        results = yield pipeline.process(dict())

        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['foo'], 42)
        self.assertEquals(len(results[0]['trace']), 2)
        self.assertEquals(dm.get_dependencies_of(pipeline[0]), [pipeline[0].pipeline_dependency])

    @defer.inlineCallbacks
    def test_dynamic_with_tracing(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pipelines.test', [{'run-pipeline':dict(pipeline_path='pipeline', trace_path='trace')}])
        cm.set('pipelines.foo', [{'set-value':dict(path='foo', value=42)}])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        dm = self.runtime_environment.dependency_manager
        test_pipeline = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()

        results = yield pipeline.process(dict(pipeline='foo'))

        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['foo'], 42)
        self.assertEquals(len(results[0]['trace']), 2)
        self.assertEquals(dm.get_dependencies_of(pipeline[0]), [])