# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import mock
from twisted.internet import defer
from twisted.python import failure
from twisted.trial import unittest

from piped import processing, exceptions, dependencies, util
from piped.processors import pipeline_processors
from piped.providers import pipeline_provider


class StubPipelineResource(object):
    def __init__(self, processor, waiter=None):
        self.processor = processor
        self.waiter = waiter
        if not waiter:
            self.waiter = lambda: self

    def __call__(self, baton):
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
        test_pipeline_evaluator = dm.add_dependency(self, dict(provider='pipeline.test'))
        dm.resolve_initial_states()

        pipeline = test_pipeline.get_resource()
        pipeline_evaluator = test_pipeline_evaluator.get_resource()

        results = yield pipeline(dict())

        self.assertEquals(results, [dict(foo=42)])

        self.assertEquals(dm.get_dependencies_of(pipeline_evaluator[0]), [pipeline_evaluator[0].pipeline_dependency])

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

        results = yield pipeline(dict(pipeline='foo'))

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
            yield pipeline(dict(pipeline='foo'))
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
            yield pipeline(dict(pipeline='foo'))
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

        results = yield pipeline(dict())

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

        results = yield pipeline(dict(pipeline='foo'))

        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['foo'], 42)
        self.assertEquals(len(results[0]['trace']), 2)
        self.assertEquals(dm.get_dependencies_of(pipeline[0]), [])


class FakePipeline(object):

    def __init__(self):
        self.batons = list()
        self.i = 0

    def __call__(self, baton):
        self.batons.append(baton)
        self.i += 1
        return [self.i]


class FakePipelineWithMultipleSinks(FakePipeline):

    def __call__(self, baton):
        super(FakePipelineWithMultipleSinks, self).__call__(baton)
        return ['pretended', 'multiple', 'results']


class FakeSlowPipeline(FakePipeline):

    @defer.inlineCallbacks
    def __call__(self, baton):
        yield util.wait(0)
        super(FakeSlowPipeline, self).__call__(baton)
        defer.returnValue([baton])


class FakeError(exceptions.PipedError):
    pass


class FakeFailingPipeline(FakePipeline):

    def __call__(self, baton):
        self.i += 1
        if not (self.i % 2):
            raise FakeError('forced failure')
        return [baton]


class TestForEach(unittest.TestCase):
    # If it takes this long, it has surely failed.
    timeout = 2

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

    # Provide some sensible defaults for the tests.
    def make_and_configure_processor(self, pipeline='pipeline_name', input_path='iterable', output_path='results', **kw):
        processor = pipeline_processors.ForEach(pipeline=pipeline, input_path=input_path, output_path=output_path, **kw)
        processor.configure(self.runtime_environment)
        return processor

    def test_depending_on_the_needed_pipeline(self):
        add_dependency = self.runtime_environment.dependency_manager.add_dependency = mock.Mock()

        processor = self.make_and_configure_processor()

        expected_dependency_spec = dict(provider='pipeline.pipeline_name')
        add_dependency.assert_called_once_with(processor, expected_dependency_spec)

    def test_for_each_invokes_the_pipeline(self):
        pipeline = FakePipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor()
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=['foo', 'bar', 'baz'])
        processor.process(baton)

        self.assertEqual(pipeline.batons, ['foo', 'bar', 'baz'])
        self.assertEqual(baton, dict(iterable=['foo', 'bar', 'baz'], results=[1, 2, 3]))

    def test_for_each_chunked(self):
        pipeline = FakePipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(chunk_size=3)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(10))
        processor.process(baton)

        chunked_range = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
        self.assertEqual(pipeline.batons, chunked_range)
        self.assertEqual(baton, dict(iterable=range(10), results=[1, 2, 3, 4]))

    def test_getting_results_of_last_sink_by_default(self):
        pipeline = FakePipelineWithMultipleSinks()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(chunk_size=2)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=[1, 2, 3])
        processor.process(baton)

        expected_results = ['results', 'results'] # two chunks times the result of the fake pipeline with multiple sinks.
        self.assertEqual(baton, dict(iterable=[1, 2, 3], results=expected_results))

    def test_custom_result_processor(self):
        pipeline = FakePipelineWithMultipleSinks()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(chunk_size=2, result_processor='input: input')
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=[1, 2, 3])
        processor.process(baton)

        expected_results = [['pretended', 'multiple', 'results']] * 2
        self.assertEqual(baton, dict(iterable=[1, 2, 3], results=expected_results))

    @defer.inlineCallbacks
    def test_processing_is_serial(self):
        pipeline = FakeSlowPipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor()
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))
        processor.process(baton)

        # FakeSlowPipeline waits one reactor-iteration before returning.
        self.assertEqual(pipeline.batons, [])
        for i in range(3):
            # Thus, we expect one additional baton to be processed for
            # every reactor iteration when the processing is serial.
            yield util.wait(0)
            self.assertEqual(pipeline.batons, range(i + 1))

    @defer.inlineCallbacks
    def test_processing_in_parallel(self):
        pipeline = FakeSlowPipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))
        processor.process(baton)

        # As opposed to the serial test, we now expect all batons to have been processed.
        self.assertEqual(pipeline.batons, [])
        yield util.wait(0)
        self.assertEqual(pipeline.batons, range(3))

    @defer.inlineCallbacks
    def test_failing_serially_but_continuing(self):
        pipeline = FakeFailingPipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor()
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        yield processor.process(baton)

        # FakeFailingPipeline fails every second item.
        self.assertEqual(baton['results'][0], 0)

        # ... so we expect the second to be a failure.
        failure_ = baton['results'][1]
        self.assertTrue(isinstance(failure_, failure.Failure))
        self.assertTrue(failure_.type is FakeError)

        # We expect that processing has continued.
        self.assertEqual(baton['results'][2], 2)

    @defer.inlineCallbacks
    def test_failing_serially_and_raising(self):
        pipeline = FakeFailingPipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(fail_on_error=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        try:
            yield processor.process(baton)
            self.fail('Expected processing to fail')

        except FakeError:
            # The baton should be unchanged.
            self.assertEqual(baton, dict(iterable=range(3)))

    @defer.inlineCallbacks
    def test_failing_in_parallel_and_stopping(self):
        pipeline = FakeFailingPipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(fail_on_error=True, parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        try:
            yield processor.process(baton)
            self.fail('Expected processing to fail')

        except FakeError:
            # The baton should be unchanged.
            self.assertEqual(baton, dict(iterable=range(3)))

    @defer.inlineCallbacks
    def test_failing_in_parallel_but_continuing(self):
        pipeline = FakeFailingPipeline()
        pipeline_resource = dependencies.InstanceDependency(pipeline)
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(fail_on_error=False, parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        yield processor.process(baton)

        # FakeFailingPipeline fails every second item.
        self.assertEqual(baton['results'][0], 0)

        # ... so we expect the second to be a failure.
        failure_ = baton['results'][1]
        self.assertTrue(isinstance(failure_, failure.Failure))
        self.assertTrue(failure_.type is FakeError)

        # We expect that processing has continued.
        self.assertEqual(baton['results'][2], 2)

    @defer.inlineCallbacks
    def test_succeeding_when_the_first_one_is_done(self):
        # Make a pipeline whose deferreds we can access.
        deferreds = list()
        batons = list()
        class FakePipeline:

            def __call__(self, baton):
                d = defer.Deferred()
                batons.append(baton)
                deferreds.append(d)
                return d

        pipeline_resource = dependencies.InstanceDependency(FakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(done_on_first=True, parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        d = processor.process(baton)

        self.assertEqual(len(deferreds), 3)
        self.assertEqual(len(batons), 3)

        # Callback one of them.
        deferreds[1].callback(['fake result'])

        yield d

        self.assertEqual(baton, dict(iterable=range(3), results='fake result'))

    @defer.inlineCallbacks
    def test_succeeding_when_the_first_one_is_done_serially(self):
        # Make a pipeline whose deferreds we can access.
        deferreds = list()
        batons = list()
        class FakePipeline:

            def __call__(self, baton):
                d = defer.Deferred()
                batons.append(baton)
                deferreds.append(d)
                return d

        pipeline_resource = dependencies.InstanceDependency(FakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(done_on_first=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        d = processor.process(baton)

        # Only one item should have been attempted processed so far.
        self.assertEqual(batons, [0])

        # Fail it:
        deferreds[0].errback(failure.Failure(FakeError('forced error')))

        # Try the second item.
        yield util.wait(0)
        self.assertEqual(batons, [0, 1])
        # ... and make it a success
        deferreds[1].callback(['fake result'])

        yield d

        # That should have resulted in not attempting the last item.
        self.assertEqual(batons, [0, 1])
        self.assertEqual(baton, dict(iterable=range(3), results='fake result'))

    @defer.inlineCallbacks
    def test_failing_when_waiting_for_one_and_none_succeed_in_serial(self):
        # Make a pipeline whose deferreds we can access.
        deferreds = list()
        batons = list()
        class FakePipeline:

            def __call__(self, baton):
                d = defer.Deferred()
                batons.append(baton)
                deferreds.append(d)
                return d

        pipeline_resource = dependencies.InstanceDependency(FakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(done_on_first=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        d = processor.process(baton)

        for i in range(3):
            self.assertEqual(len(deferreds), i + 1)
            deferreds[-1].errback(failure.Failure(FakeError('forced error')))

        try:
            yield d
            self.fail('Expected failure')

        except exceptions.AllPipelinesFailedError, e:
            self.assertEqual(len(e.failures), 3)
            for f in e.failures:
                self.assertTrue(f.type, FakeError)

    @defer.inlineCallbacks
    def test_failing_when_waiting_for_one_and_none_succeed_in_parallel(self):
        # Make a pipeline whose deferreds we can access.
        deferreds = list()
        batons = list()
        class FakePipeline:

            def __call__(self, baton):
                d = defer.Deferred()
                batons.append(baton)
                deferreds.append(d)
                return d

        pipeline_resource = dependencies.InstanceDependency(FakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(done_on_first=True, parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=range(3))

        d = processor.process(baton)

        self.assertEqual(len(deferreds), 3)
        self.assertEqual(len(batons), 3)

        for d_to_fail in deferreds:
            d_to_fail.errback(failure.Failure(FakeError('forced error')))

        try:
            yield d
            self.fail('Expected failure')

        except exceptions.AllPipelinesFailedError, e:
            self.assertEqual(len(e.failures), 3)
            for f in e.failures:
                self.assertTrue(f.type, FakeError)

    @defer.inlineCallbacks
    def test_handles_empty_input_serially(self):
        pipeline_resource = dependencies.InstanceDependency(FakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor()
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=list())

        yield processor.process(baton)

        self.assertEqual(baton, dict(iterable=list(), results=list()))

    @defer.inlineCallbacks
    def test_handles_empty_input_in_parallel(self):
        pipeline_resource = dependencies.InstanceDependency(FakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=list())

        yield processor.process(baton)

        self.assertEqual(baton, dict(iterable=list(), results=list()))

    @defer.inlineCallbacks
    def test_dict_input(self):
        class CustomFakePipeline(FakePipeline):
            def __call__(self, input):
                return [dict(bar=42, baz=93).get(input)]

        pipeline_resource = dependencies.InstanceDependency(CustomFakePipeline())
        pipeline_resource.is_ready = True

        processor = self.make_and_configure_processor(parallel=True)
        processor.pipeline_dependency = pipeline_resource

        baton = dict(iterable=dict(foo='bar', bar='baz'))
        yield processor.process(baton)

        # the pipeline should only be invoked with the values, but the output should still be a dict
        self.assertEqual(baton, dict(iterable=dict(foo='bar', bar='baz'), results=dict(foo=42, bar=93)))