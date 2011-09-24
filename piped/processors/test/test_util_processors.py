# encoding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import copy
import time

import mock
from twisted.internet import defer
from twisted.trial import unittest
from twisted.python import failure, reflect

from piped import decorators, dependencies, util, processing, exceptions
from piped.processors import util_processors


class FakeError(exceptions.PipedError):
    pass


class TestMergeWithDict(unittest.TestCase):

    def test_merge_with_dict(self):
        """ When merging a baton and a dictionary, the result should
        be a new dictionary, with the original baton left intact. """
        baton = dict(foo='bar', answer=42)
        merge_with = dict(a='b', foo='foo')
        processor = util_processors.MergeWithDictProcessor(dict=merge_with, merge_args=dict(inline=False))
        result = processor.process(baton)
        self.assertNotEquals(result, baton, "Processor unexpectedly modified the original baton")
        self.assertEquals(result, dict(foo=['bar', 'foo'], answer=42, a='b'))

    def test_merge_with_dict_and_merge_args(self):
        """ As opposed to the previous test, we now require that the
        baton is modified in place. """
        baton = dict(foo='bar', answer=42)
        merge_with = dict(a='b', foo='foo')
        merge_args = dict(inline=True)
        processor = util_processors.MergeWithDictProcessor(dict=merge_with, merge_args=merge_args)
        result = processor.process(baton)
        # We did specify inline=True.
        self.assertEquals(result, baton, "Expected the original baton to be modified")
        self.assertEquals(result, dict(foo=['bar', 'foo'], answer=42, a='b'))


class TestCallNamedAny(unittest.TestCase):

    def test_named_any_simple(self):
        """ Test that the processor can successfully instantiate an instance of its own class. """
        processor_name = reflect.fullyQualifiedName(util_processors.CallNamedAny)
        processor = util_processors.CallNamedAny(name=processor_name, kwargs=dict(name='test_name'))

        result = processor.process(dict())

        self.assertIsInstance(result['result'], util_processors.CallNamedAny)
        self.assertEquals(result['result'].name, 'test_name')

    def test_named_any_with_custom_output_path(self):
        """ Test that the processor sets the expected key in the baton. """
        processor_name = reflect.fullyQualifiedName(util_processors.CallNamedAny)
        processor = util_processors.CallNamedAny(name=processor_name, kwargs=dict(name='test_name'), output_path='custom')

        result = processor.process(dict())

        self.assertIsInstance(result['custom'], util_processors.CallNamedAny)
        self.assertEquals(result['custom'].name, 'test_name')

    def test_named_any_replacing_baton(self):
        """ Test that the processor can replace the baton. """
        processor_name = reflect.fullyQualifiedName(util_processors.CallNamedAny)
        processor = util_processors.CallNamedAny(name=processor_name, kwargs=dict(name='test_name'), output_path='')

        result = processor.process(dict())

        self.assertIsInstance(result, util_processors.CallNamedAny)
        self.assertEquals(result.name, 'test_name')


class TestCallbackDeferred(unittest.TestCase):

    def test_callback_result_defaults(self):
        processor = util_processors.CallbackDeferred()
        d = defer.Deferred()

        processor.process(dict(deferred=d))
        self.assertTrue(d.called)
        self.assertEquals(d.result, None)

    def test_callback_result_with_explicit_result(self):
        processor = util_processors.CallbackDeferred(result=42)
        d = defer.Deferred()

        processor.process(dict(deferred=d))
        self.assertTrue(d.called)
        self.assertEquals(d.result, 42)

    def test_callback_result_with_result_in_baton(self):
        processor = util_processors.CallbackDeferred()
        d = defer.Deferred()

        processor.process(dict(deferred=d, result=93))
        self.assertTrue(d.called)
        self.assertEquals(d.result, 93)

class TestCoroutineWrapper(unittest.TestCase):

    def test_coroutine_wrapper(self):
        """ Test wrapping of a very simple coroutine, that only
        appends its batons to a list. """
        result = []

        @decorators.coroutine
        def wrapme():
            while True:
                baton = yield
                result.append(baton)

        processor = util_processors.CoroutineWrapper(wrapme())
        processor.process(1)
        processor.process(2)
        self.assertEquals(result, [1, 2])


class TestRemapProcessor(unittest.TestCase):

    @defer.inlineCallbacks
    def test_remap_processor(self):
        """ Check that the resulting dictionary contains the new
        fields as defined by the mapping. """
        baton = dict(a='b', c='d', e=dict(f='g'))
        mapping = {'e.f':'h', 'a': 'i'}
        expected_result = dict(baton)
        result = yield util_processors.RemapProcessor(mapping=mapping).process(baton)
        expected_result['h'] = 'g'
        expected_result['i'] = 'b'
        self.assertEquals(result, expected_result)

    @defer.inlineCallbacks
    def test_nonexistent_inputs(self):
        """ Check that nonexistent inputs do not affect the output. """
        baton = dict(a='b', c='d', e=dict(f='g'))
        mapping = {'nothing':'c', 'else':'c'}
        expected_result = dict(baton)
        result = yield util_processors.RemapProcessor(mapping=mapping).process(baton)
        self.assertEquals(result, expected_result)


class TestBatonCollector(unittest.TestCase):

    def test_baton_collector(self):
        """ Test that the collector correctly collects batons. """
        l = []
        processor = util_processors.BatonCollector(l)
        processor.process(1)
        processor.process(2)
        self.assertEquals(l, [1, 2])

    def test_baton_collector_without_deepcopy(self):
        """ By default the collector should not copy the batons passed
        to it, so if they are subsequently modified, those
        modifications should be visible in the collected batons. """
        l = []
        a = [1, 2]
        b = [3, 4]
        processor = util_processors.BatonCollector(l)
        processor.process(a)
        processor.process(b)
        a.pop()
        b.pop()
        self.assertEquals(l, [[1], [3]])

    def test_baton_collector_with_deepcopy(self):
        """ When we specify *deepcopy=True*, however, the batons
        should be as they were when they passed through the
        collector. """
        l = []
        a = [1, 2]
        b = [3, 4]
        processor = util_processors.BatonCollector(l, deepcopy=True)
        processor.process(a)
        processor.process(b)
        a.pop()
        b.pop()
        self.assertEquals(l, [[1, 2], [3, 4]])


class TestBatonCleaner(unittest.TestCase):

    def test_baton_cleaner_remove_only(self):
        """ Test only specifying something to remove in the cleaner. """
        baton = dict(a='b', c='d')
        processor = util_processors.BatonCleaner(remove=['c'])
        processor.process(baton)
        self.assertEquals(baton, dict(a='b'))

    def test_baton_cleaner_keep_only(self):
        """ Test only specifying something to keep in the cleaner. """
        baton = dict(a='b', c='d')
        processor = util_processors.BatonCleaner(keep=['a'])
        processor.process(baton)
        self.assertEquals(baton, dict(a='b'))

    def test_baton_cleaner_remove_and_keep(self):
        """ Test providing both something to keep and to remove. """
        baton = dict(a='b', c='d')
        processor = util_processors.BatonCleaner(keep=['a'], remove=['non-existing'])
        processor.process(baton)
        self.assertEquals(baton, dict(a='b'))

        baton = dict(a='b', c='d')
        processor = util_processors.BatonCleaner(keep=['a'], remove=['non-existing'])
        processor.process(baton)
        self.assertEquals(baton, dict(a='b'))

    def test_baton_cleaner_err_when_misconfigured(self):
        """ It doesn't make sense to have a BatonCleaner with an empty
        configuration. """
        try:
            util_processors.BatonCleaner()
            self.fail("Expected BatonCleaner to err.")
        except AssertionError:
            pass


class TestTrapFailure(unittest.TestCase):

    def _create_processor(self, **kwargs):
        return util_processors.TrapFailure(**kwargs)

    def test_simple_trapping(self):
        processor = self._create_processor(error_types=reflect.fullyQualifiedName(FakeError), output_path='trapped')

        try:
            raise FakeError('test')
        except FakeError as fe:
            baton = processor.process(dict())
            self.assertEquals(baton['trapped'], FakeError)

    def test_not_trapping_unexpected_exceptions(self):
        processor = self._create_processor(error_types=reflect.fullyQualifiedName(FakeError))

        try:
            raise exceptions.PipedError('test')
        except exceptions.PipedError as pe:
            try:
                processor.process(None)
            except failure.Failure as reason:
                self.assertEquals(reason.type, exceptions.PipedError)
                self.assertEquals(reason.value, pe)
            else:
                self.fail('Expected a failure to be raised.')

    def test_no_current_failure(self):
        processor = self._create_processor(error_types=reflect.fullyQualifiedName(FakeError))

        self.assertRaises(failure.NoCurrentExceptionError, processor.process, None)

    def test_trapping_multiple_types(self):
        error_types = [reflect.fullyQualifiedName(FakeError), reflect.fullyQualifiedName(exceptions.ConfigurationError)]
        processor = self._create_processor(error_types=error_types, output_path='trapped')

        for error_type in (FakeError, exceptions.ConfigurationError):
            try:
                raise error_type('test')
            except error_type as fe:
                baton = processor.process(dict())
                self.assertEquals(baton['trapped'], error_type)


class TestFlattenDictionaryList(unittest.TestCase):

    @defer.inlineCallbacks
    def test_dictionary_is_flattened(self):
        processor = util_processors.FlattenDictionaryList(key_path='author', input_path='foo.bar')
        baton = dict(foo=dict(bar=[dict(author='author1'), dict(author='author2')]))
        yield processor.process(baton)
        self.assertEquals(baton['foo']['bar'], ['author1', 'author2'])


class TestLambdaProcessor(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

    @defer.inlineCallbacks
    def test_lambda_processor(self):
        configuration = dict(input_path='foo', output_path='bar')
        configuration['lambda'] = 'value: value[::-1]'
        processor = util_processors.LambdaProcessor(**configuration)
        processor.configure(self.runtime_environment)
        baton = dict(foo='123')
        yield processor.process(baton)
        self.assertEquals(baton, dict(foo='123', bar='321'))

    @defer.inlineCallbacks
    def test_lambda_processor_overwrites(self):
        configuration = dict(input_path='foo')
        configuration['lambda'] = 'value: value[::-1]'
        processor = util_processors.LambdaProcessor(**configuration)
        processor.configure(self.runtime_environment)
        baton = dict(foo='123')
        yield processor.process(baton)
        self.assertEquals(baton, dict(foo='321'))


class TestExecProcessor(unittest.TestCase):

    @defer.inlineCallbacks
    def test_simple_usage(self):
        configuration = dict(input_path='foo', output_path='bar', code='return self')
        processor = util_processors.ExecProcessor(**configuration)
        processor.configure(processing.RuntimeEnvironment())
        baton = dict(foo='123')
        yield processor.process(baton)
        self.assertEquals(baton, dict(foo='123', bar=processor))

    @defer.inlineCallbacks
    def test_inline_callbacks(self):
        configuration = dict(
            inline_callbacks=True,
            code = """
                yield util.wait(0)
                defer.returnValue('done')
            """
        )
        processor = util_processors.ExecProcessor(**configuration)
        processor.configure(processing.RuntimeEnvironment())
        result = yield processor.process(None)
        self.assertEquals(result, 'done')

    @defer.inlineCallbacks
    def test_indenting(self):
        configuration = dict(
            code = """ bar = 42
                return bar
            """
        )
        processor = util_processors.ExecProcessor(**configuration)
        processor.configure(processing.RuntimeEnvironment())
        result = yield processor.process(None)
        self.assertEquals(result, 42)

    @defer.inlineCallbacks
    def test_indenting_with_nested_blocks(self):
        configuration = dict(
            code = """ def inner():
                    def nested():
                        return 40
                    return nested()+2
                return inner()
            """
        )
        processor = util_processors.ExecProcessor(**configuration)
        processor.configure(processing.RuntimeEnvironment())
        result = yield processor.process(None)
        self.assertEquals(result, 42)

    def test_dependencies(self):
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        class Provider(object):
            def add_consumer(self, resource_dependency):
                resource_dependency.on_resource_ready('test_dependency_resource')
        runtime_environment.resource_manager.register('test_dependency', Provider())

        configuration = dict(
            code = 'return 123',
            dependencies = dict(test_dependency='test_dependency')
        )
        processor = util_processors.ExecProcessor(**configuration)
        processor.configure(runtime_environment)

        runtime_environment.dependency_manager.resolve_initial_states()

        self.assertEquals(processor.dependencies.test_dependency, 'test_dependency_resource')

    @defer.inlineCallbacks
    def test_namespace(self):
        configuration = dict(
            namespace = dict(
                processors = 'piped.processors.util_processors'
            ),
            code = 'return processors'
        )
        processor = util_processors.ExecProcessor(**configuration)
        processor.configure(processing.RuntimeEnvironment())
        result = yield processor.process(None)
        self.assertEquals(result, util_processors)


class TestDictGrouper(unittest.TestCase):

    def test_dict_grouper_groups_iterable_of_dicts(self):
        processor = util_processors.DictGrouper(key_path='key', input_path='list_of_dicts', output_path='grouped', fallback='fallback')

        group_a = [dict(key='a', n=1), dict(key='a', n=3)]
        group_b = [dict(key='b', n=2), dict(key='b', n=4)]
        # Interleave the dicts, so the processor does not rely on the list being sorted.
        baton = dict(list_of_dicts=util.interleaved(group_a, group_b))

        # Copy it, so we can assert it's not modified
        original_input = copy.deepcopy(baton['list_of_dicts'])
        processor.process(baton)

        # The result should be grouped
        self.assertEquals(baton['grouped'], dict(a=group_a, b=group_b))
        # The input should be untouched
        self.assertEquals(baton['list_of_dicts'], original_input)

    def test_dict_grouper_groups_reversed_iterable_of_dicts(self):
        processor = util_processors.DictGrouper(key_path='key', input_path='list_of_dicts', output_path='grouped', fallback='fallback')

        group_a = [dict(key='a', n=3), dict(key='a', n=1)]
        group_b = [dict(key='b', n=4), dict(key='b', n=2)]
        # Interleave the dicts, so the processor does not rely on the list being sorted.
        baton = dict(list_of_dicts=util.interleaved(group_a, group_b))

        # Copy it, so we can assert it's not modified
        original_input = copy.deepcopy(baton['list_of_dicts'])
        processor.process(baton)

        # The result should be grouped
        self.assertEquals(baton['grouped'], dict(a=group_a, b=group_b))
        # The input should be untouched
        self.assertEquals(baton['list_of_dicts'], original_input)

    def test_dict_grouper_overwriting_input(self):
        processor = util_processors.DictGrouper(key_path='key', input_path='input', fallback='fallback')

        group_a = [dict(key='a', n=1), dict(key='a', n=3)]
        group_b = [dict(key='b', n=2), dict(key='b', n=4)]
        # Interleave the dicts, so the processor does not rely on the list being sorted.
        baton = dict(input=util.interleaved(group_a, group_b))

        processor.process(baton)

        # The result should be grouped
        self.assertEquals(baton, dict(input=dict(a=group_a, b=group_b)))

    def test_dict_grouper_fallbacking(self):
        processor = util_processors.DictGrouper(key_path='key', input_path='input', fallback='fallback')

        group_a = [dict(key='a', n=1), dict(key='a', n=3)]
        group_b = [dict(key='b', n=2), dict(key='b', n=4)]
        should_be_in_fallback = [dict(n=5), dict(n=6)]
        # Interleave the dicts, so the processor does not rely on the list being sorted.
        baton = dict(input=util.interleaved(group_a, group_b, should_be_in_fallback))

        processor.process(baton)

        # The result should be grouped
        self.assertEquals(baton, dict(input=dict(a=group_a, b=group_b, fallback=should_be_in_fallback)))

    def test_dict_grouper_with_no_input(self):
        processor = util_processors.DictGrouper(key_path='key', input_path='input', fallback='fallback')
        baton = dict()
        processor.process(baton)
        self.assertEquals(baton, dict(input=dict()))


class TestNestedListFlattener(unittest.TestCase):

    def test_flatten_nested_lists(self):
        processor = util_processors.NestedListFlattener(paths=['foo', 'bar'])
        baton = dict(foo=[[1, 2], [3], [4, 5, [6, 7, [8]]]], bar='iterable string')
        processor.process(baton)
        self.assertEquals(baton, dict(foo=[1, 2, 3, 4, 5, 6, 7, 8], bar='iterable string'))

    def test_flatten_with_prefix(self):
        processor = util_processors.NestedListFlattener(paths=['foo', 'bar'], path_prefix='baz.')
        baton = dict(baz=dict(foo=[[1, 2], [3], [4, 5, [6, 7, [8]]]], bar='iterable string'))
        processor.process(baton)
        self.assertEquals(baton, dict(baz=dict(foo=[1, 2, 3, 4, 5, 6, 7, 8], bar='iterable string')))

    def test_no_flatten_required(self):
        processor = util_processors.NestedListFlattener(paths=['foo'])
        baton = dict(foo=[1, 2, 3])
        processor.process(baton)
        self.assertEquals(baton, dict(foo=[1, 2, 3]))

    def test_path_is_empty_or_nonexisting(self):
        processor = util_processors.NestedListFlattener(paths=['foo', 'bar'])
        baton = dict(foo=[])
        processor.process(baton)
        self.assertEquals(baton, dict(foo=[]))


class TestStopper(unittest.TestCase):

    def test_stops_when_it_should(self):
        stopper = util_processors.Stopper(decider='baton: True')

        stopper.consumers = ['not empty']

        stopper.configure(processing.RuntimeEnvironment())

        baton = object()
        self.assertEquals(stopper.get_consumers(baton), [])

    def test_continues_when_it_should(self):
        stopper = util_processors.Stopper(decider='baton: False')
        stopper.consumers = ['not empty']

        stopper.configure(processing.RuntimeEnvironment())

        baton = object()
        self.assertEquals(stopper.get_consumers(baton), ['not empty'])

    def test_doesnt_touch_the_baton(self):
        stopper = util_processors.Stopper(decider='baton: False')
        stopper.consumers = ['not empty']

        stopper.configure(processing.RuntimeEnvironment())

        baton = object()
        self.assertTrue(stopper.process(baton) is baton)


class TestStringFormatter(unittest.TestCase):

    @defer.inlineCallbacks
    def test_simple_formatting(self):
        processor = util_processors.StringFormatter(format='{0} formatted')
        result = yield processor.process('simple')
        self.assertEquals(result, 'simple formatted')

    @defer.inlineCallbacks
    def test_simple_formatting_format_path(self):
        processor = util_processors.StringFormatter(format_path='format')
        result = yield processor.process(dict(format='{text} formatted', text='simple'))
        self.assertEquals(result, 'simple formatted')

    @defer.inlineCallbacks
    def test_formatting_list(self):
        processor = util_processors.StringFormatter(format='{0} and {1}')
        result = yield processor.process([42, '93'])
        self.assertEquals(result, '42 and 93')

    @defer.inlineCallbacks
    def test_formatting_list_without_unpacking(self):
        processor = util_processors.StringFormatter(format='{0[0]} and {0[1]}', unpack=False)
        result = yield processor.process([42, '93'])
        self.assertEquals(result, '42 and 93')

    @defer.inlineCallbacks
    def test_formatting_dict(self):
        processor = util_processors.StringFormatter(format='{a} and {b}')
        result = yield processor.process(dict(a=42, b=93))
        self.assertEquals(result, '42 and 93')

    @defer.inlineCallbacks
    def test_formatting_dict_without_unpacking(self):
        processor = util_processors.StringFormatter(format='{0[a]} and {0[b]}', unpack=False)
        result = yield processor.process(dict(a=42, b=93))
        self.assertEquals(result, '42 and 93')

    def test_invalid_configurations(self):
        self.assertRaises(exceptions.ConfigurationError, util_processors.StringFormatter)
        self.assertRaises(exceptions.ConfigurationError, util_processors.StringFormatter, format='', format_path='')


class TestStringPrefixer(unittest.TestCase):

    def test_prefixing(self):
        formatter = util_processors.StringPrefixer(prefix='foo: ', input_path='value')
        baton = dict(value='bar')
        formatter.process(baton)

        self.assertEquals(baton, dict(value='foo: bar'))

    def test_prefixing_when_value_is_not_string(self):
        formatter = util_processors.StringPrefixer(prefix='foo: ', input_path='value')
        baton = dict(value=42)
        formatter.process(baton)

        self.assertEquals(baton, dict(value='foo: 42'))

    def test_prefixing_when_neither_value_nor_prefix_is_string(self):
        formatter = util_processors.StringPrefixer(prefix=42, input_path='value')
        baton = dict(value=42)
        formatter.process(baton)

        self.assertEquals(baton, dict(value='4242'))


class StubException(exceptions.PipedError):
    pass


class TestRaiseException(unittest.TestCase):

    def test_raises_default_exception(self):
        raiser = util_processors.RaiseException()
        self.assertRaises(Exception, raiser.process, object())

    def test_raises_custom_exception(self):
        exception_type = reflect.fullyQualifiedName(StubException)
        kwargs = dict(msg='e_msg', detail='detail', hint='hint')
        raiser = util_processors.RaiseException(type=exception_type, kwargs=kwargs)

        try:
            raiser.process(object())
            self.fail('Expected the processor to raise an exception')
        except StubException, e:
            for k, v in kwargs.items():
                self.assertEquals(getattr(e, k), v)


class FakePipeline(object):

    def __init__(self):
        self.batons = list()
        self.i = 0

    def process(self, baton):
        self.batons.append(baton)
        self.i += 1
        return [self.i]


class FakePipelineWithMultipleSinks(FakePipeline):

    def process(self, baton):
        FakePipeline.process(self, baton)
        return ['pretended', 'multiple', 'results']


class FakeSlowPipeline(FakePipeline):

    @defer.inlineCallbacks
    def process(self, baton):
        yield util.wait(0)
        FakePipeline.process(self, baton)
        defer.returnValue([baton])


class FakeFailingPipeline(FakePipeline):

    def process(self, baton):
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
        processor = util_processors.ForEach(pipeline=pipeline, input_path=input_path, output_path=output_path, **kw)
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

            def process(self, baton):
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

            def process(self, baton):
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

            def process(self, baton):
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

            def process(self, baton):
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


class TestMappingSetter(unittest.TestCase):

    def test_setting_mapping(self):
        processor = util_processors.MappingSetter(mapping={'foo.bar': 42, 'key': [1, 2, 3]})
        baton = dict(key='value')
        processor.process(baton)
        self.assertEquals(baton, dict(foo=dict(bar=42), key=[1, 2, 3]))

    def test_setting_mapping_with_path_prefix(self):
        processor = util_processors.MappingSetter(mapping=dict(foo=1, bar=2), path_prefix='nested.')
        baton = dict(key='value')
        processor.process(baton)
        self.assertEquals(baton, dict(key='value', nested=dict(foo=1, bar=2)))


class TestValueSetter(unittest.TestCase):

    def test_setting_value(self):
        baton = dict(key='value')
        processor = util_processors.ValueSetter(path='nested.something', value='value')
        processor.process(baton)
        self.assertEquals(baton, dict(key='value', nested=dict(something='value')))

    def test_overwriting_value(self):
        baton = dict(key='value')
        processor = util_processors.ValueSetter(path='key', value='not value')
        processor.process(baton)
        self.assertEquals(baton, dict(key='not value'))


class TestCounterIncrementer(unittest.TestCase):

    def test_increasing(self):
        baton = dict(nested=dict(counter=1))
        processor = util_processors.CounterIncrementer(counter_path='nested.counter')
        processor.process(baton)
        self.assertEquals(baton, dict(nested=dict(counter=2)))
        processor.process(baton)
        self.assertEquals(baton, dict(nested=dict(counter=3)))

    def test_increasing_with_custom_increment(self):
        baton = dict(nested=dict(counter=1))
        processor = util_processors.CounterIncrementer(counter_path='nested.counter', increment=2)
        processor.process(baton)
        self.assertEquals(baton, dict(nested=dict(counter=3)))
        processor.process(baton)
        self.assertEquals(baton, dict(nested=dict(counter=5)))

    def test_decreasing(self):
        baton = dict(counter=1)
        processor = util_processors.CounterIncrementer(counter_path='counter', increment=-1)
        processor.process(baton)
        self.assertEquals(baton, dict(counter=0))
        processor.process(baton)
        self.assertEquals(baton, dict(counter=-1))


class TestLogger(unittest.TestCase):

    def test_complaining_about_invalid_log_level(self):
        self.assertRaises(ValueError, util_processors.Logger, message='log message', level='invalid')

    def test_complaining_about_both_message_and_path(self):
        self.assertRaises(exceptions.ConfigurationError, util_processors.Logger, message='', message_path='path')

    def test_complaining_about_neither_message_nor_path(self):
        self.assertRaises(exceptions.ConfigurationError, util_processors.Logger)

    def test_logging_with_default_level(self):
        with mock.patch('piped.processors.util_processors.log') as mocked_log:
            logger = util_processors.Logger(message='log message')
            logger.process(dict())

        mocked_log.info.assert_called_once_with('log message')

    def test_logging_with_custom_level(self):
        with mock.patch('piped.processors.util_processors.log') as mocked_log:
            logger = util_processors.Logger(message='error message', level='error')
            logger.process(dict())

        mocked_log.error.assert_called_once_with('error message')

    def test_logging_with_path(self):
        with mock.patch('piped.processors.util_processors.log') as mocked_log:
            logger = util_processors.Logger(message_path='log.message')
            logger.process(dict(log=dict(message='log message')))

        mocked_log.info.assert_called_once_with('log message')

    def test_not_logging_when_no_message(self):
        with mock.patch('piped.processors.util_processors.log') as mocked_log:
            logger = util_processors.Logger(message_path='log.message')
            logger.process(dict())

        self.assertEquals(mocked_log.method_calls, [])


__doctests__ = [util_processors]
