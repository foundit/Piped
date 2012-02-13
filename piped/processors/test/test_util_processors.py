# encoding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import copy
import time

import mock
from twisted.internet import defer
from twisted.trial import unittest
from twisted.python import failure, reflect

from piped import decorators, dependencies, util, processing, exceptions, yamlutil
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
            self.assertEquals(baton['trapped'].value, fe)

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
                self.assertEquals(baton['trapped'].value, fe)


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

class DependencyCallerTest(unittest.TestCase):
    class TestResource(object):
        def __call__(self):
            return 'test __call__'

        def echo(self, *a, **kw):
            return [a, kw]

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()
        self.runtime_environment.resource_manager.register('test', provider=self)

    def add_consumer(self, resource_dependency):
        resource_dependency.on_resource_ready(self.TestResource())

    def _create_processor(self, **config):
        return util_processors.DependencyCaller(**config)

    @defer.inlineCallbacks
    def test_simple_calling(self):
        processor = self._create_processor(dependency='test', output_path='result')
        processor.configure(self.runtime_environment)
        self.runtime_environment.dependency_manager.resolve_initial_states()

        baton = yield processor.process(dict())
        self.assertEquals(baton['result'], 'test __call__')

    @defer.inlineCallbacks
    def test_simple_argument(self):
        processor = self._create_processor(dependency='test', method='echo', arguments=dict(foo='bar'), output_path='result')
        processor.configure(self.runtime_environment)
        self.runtime_environment.dependency_manager.resolve_initial_states()

        baton = yield processor.process(dict())
        self.assertEquals(baton['result'], [(dict(foo='bar'), ), dict()])

    @defer.inlineCallbacks
    def test_argument_unpacking(self):
        processor = self._create_processor(dependency='test', method='echo', arguments=dict(foo='bar'), unpack_arguments=True, output_path='result')
        processor.configure(self.runtime_environment)
        self.runtime_environment.dependency_manager.resolve_initial_states()

        baton = yield processor.process(dict())
        self.assertEquals(baton['result'], [tuple(), dict(foo='bar')])

    @defer.inlineCallbacks
    def test_baton_argument(self):
        processor = self._create_processor(dependency='test', method='echo', arguments=yamlutil.BatonPath(), output_path='result')
        processor.configure(self.runtime_environment)
        self.runtime_environment.dependency_manager.resolve_initial_states()

        baton = yield processor.process(dict(foo='bar'))
        # the result will contain a reference to itself because the output is set in the same dict instance as the first argument.
        self.assertEquals(baton['result'], [(dict(foo='bar', result=baton['result']), ), dict()])

    @defer.inlineCallbacks
    def test_baton_argument_unpacking(self):
        processor = self._create_processor(dependency='test', method='echo', arguments=yamlutil.BatonPath(), unpack_arguments=True, output_path='result')
        processor.configure(self.runtime_environment)
        self.runtime_environment.dependency_manager.resolve_initial_states()

        baton = yield processor.process(dict(foo='bar'))
        self.assertEquals(baton['result'], [tuple(), dict(foo='bar')])

    @defer.inlineCallbacks
    def test_baton_argument_unpacking_with_list_arguments(self):
        processor = self._create_processor(dependency='test', method='echo', arguments=['foo', 'bar'], unpack_arguments=True, output_path='result')
        processor.configure(self.runtime_environment)
        self.runtime_environment.dependency_manager.resolve_initial_states()

        baton = yield processor.process(baton=dict())
        self.assertEquals(baton['result'], [('foo', 'bar'), dict()])


__doctests__ = [util_processors]
