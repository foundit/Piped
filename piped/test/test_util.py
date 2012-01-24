# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Tests for utilities """
import copy
import datetime

from twisted.internet import defer
from twisted.trial import unittest

from piped import util, processing


class uncopyable(object):
    def __deepcopy__(self, memo):
        raise copy.Error("Intentional copy error")


class TestSafeDeepCopy(unittest.TestCase):

    def test_safe_deepcopy(self):

        problem_object = uncopyable()
        d = dict(foo='bar', nested=dict(unsafe=problem_object), _volatile=['nested.unsafe'])
        d2 = util.safe_deepcopy(d)

        expected_copy = dict(foo='bar', nested=dict(unsafe=repr(problem_object)), _volatile=['nested.unsafe'])
        self.assertEquals(d2, expected_copy)
        self.assertEquals(d, dict(foo='bar', nested=dict(unsafe=problem_object), _volatile=['nested.unsafe']))

        # Ensure that the original object is not mangled:
        d = dict(foo='bar', nested=dict(unsafe=problem_object))
        self.assertRaises(copy.Error, util.safe_deepcopy, d)
        self.assertEquals(d, dict(foo='bar', nested=dict(unsafe=problem_object)))

        # Test custom object describer
        d = dict(foo='bar', nested=dict(unsafe=problem_object), _volatile=['nested.unsafe'])
        d2 = util.safe_deepcopy(d, object_describer=id)
        expected_copy = dict(foo='bar', nested=dict(unsafe=id(problem_object)), _volatile=['nested.unsafe'])
        self.assertEquals(d2, expected_copy)

    def test_safe_deepcopy_on_primitives(self):
        self.assertEquals(util.safe_deepcopy('foo'), 'foo')
        self.assertEquals(util.safe_deepcopy(42), 42)
        self.assertEquals(util.safe_deepcopy([1,2]), [1,2])

    def test_safe_deepcopy_with_nonexisting_path(self):
        problem_object = uncopyable()
        data = dict(_volatile=['bar', 'unsafe'], foo=42, unsafe=problem_object)

        # bar does not exist in the original, but that should not cause errors
        copy = util.safe_deepcopy(data, object_describer=id)
        expected_copy = dict(_volatile=['bar', 'unsafe'], foo=42, unsafe=id(problem_object))
        self.assertEquals(copy, expected_copy)


class MergeDictsTest(unittest.TestCase):

    def test_simple(self):
        self.assertEquals(dict(foo='bar'), util.merge_dicts(dict(), dict(foo='bar')))
        self.assertEquals(dict(foo='bar'), util.merge_dicts(dict(foo='bar'), dict()))

    def test_merge_two_primitives(self):
        self.assertEquals(dict(foo=['bar', 'baz']), util.merge_dicts(dict(foo='bar'), dict(foo='baz')))
        self.assertEquals(dict(foo=['baz', 'bar']), util.merge_dicts(dict(foo='baz'), dict(foo='bar')))
        self.assertEquals(dict(foo='bar'), util.merge_dicts(dict(foo='baz'), dict(foo='bar'), replace=True))

    def test_replace_primitives(self):
        self.assertEquals(dict(foo='bar'), util.merge_dicts(dict(foo='baz'), dict(foo='bar'), replace_primitives=True))
        self.assertEquals(dict(foo='baz'), util.merge_dicts(dict(foo='bar'), dict(foo='baz'), replace_primitives=True))

        c = dict(bar='baz')
        d = dict(bar='foo')
        a = dict(foo=c)
        b = dict(foo=d)
        self.assertEquals(dict(foo=dict(bar='foo')), util.merge_dicts(a, b, replace_primitives=True))
        self.assertEquals(dict(foo=dict(bar=['baz', 'foo'])), util.merge_dicts(a, b))
        self.assertEquals(dict(foo=dict(bar='foo')), util.merge_dicts(a, b, replace=True))

        e = dict(foo=dict(bar=c, baz=d))
        f = dict(foo=dict(bar=d))

        self.assertEquals(dict(foo=dict(bar=dict(bar='foo'))), util.merge_dicts(e, f, replace=True))
        self.assertEquals(dict(foo=dict(bar=dict(bar='foo'), baz=dict(bar='foo'))), util.merge_dicts(e, f, replace_primitives=True))

    def test_inline(self):
        a = dict(foo='bar')
        b = dict(bar='baz')
        util.merge_dicts(a, b, inline=True)
        self.assertIn('bar', a)

    def test_lowercase(self):
        a = dict(FOO='bar')
        b = dict(BAR='baz')
        util.merge_dicts(a, b, inline=True)
        self.assertIn('FOO', a)
        self.assertIn('BAR', a)
        self.assertNotIn('foo', a)
        self.assertNotIn('bar', a)

    def test_nesting(self):
        a = dict(foo=['bar', 'baz'])
        b = dict(foo=['baz', 'foo'])
        self.assertEquals(dict(foo=['bar', 'baz', 'baz', 'foo']), util.merge_dicts(a, b))
        self.assertEquals(dict(foo=['baz', 'foo']), util.merge_dicts(a, b, replace=True))


class TestDictGetPath(unittest.TestCase):

    def test_get_fallback_when_first_not_found(self):
        self.assertEquals(util.dict_get_path(dict(), 'some.path', 'fallback'), 'fallback')

    def test_plain_get(self):
        self.assertEquals(util.dict_get_path(dict(key='value'), 'key'), 'value')

    def test_with_none(self):
        self.assertEquals(util.dict_get_path(dict(foo=None), 'foo.bar', 'fallback'), 'fallback')

    def test_get_nested(self):
        self.assertEquals(util.dict_get_path(dict(foo=dict(bar='baz')), 'foo'), dict(bar='baz'))
        self.assertEquals(util.dict_get_path(dict(foo=dict(bar='baz')), 'foo.bar'), 'baz')
        self.assertEquals(util.dict_get_path(dict(foo=dict(bar='baz')), 'foo.bar.baz'), None)
        self.assertEquals(util.dict_get_path(dict(foo=dict(bar='baz')), 'foo.bar.baz', 'fallback'), 'fallback')
        self.assertEquals(util.dict_get_path(dict(foo=dict(bar='baz')), ('foo', 'bar', 'baz'), 'fallback'), 'fallback')

    def test_custom_separator(self):
        self.assertEquals(util.dict_get_path(dict(foo=dict(bar='baz')), 'foo/bar', separator='/'), 'baz')

    def test_not_a_dictionary(self):

        class Foo(object):
            def __getitem__(self, key):
                if key == 'foo':
                    return self
                return key

        foo = Foo()
        self.assertEquals(util.dict_get_path(foo, 'foo.foo'), foo)
        self.assertEquals(util.dict_get_path(foo, 'foo.foo.bar'), 'bar')

    def test_getting_the_root(self):
        d = dict(a=dict(b=42))
        self.assertTrue(util.dict_get_path(d, '') is d)

    def test_getting_attribute(self):

        class Bar(object):
            def __init__(self, wrapped):
                self.wrapped = wrapped

        d = dict(a=Bar(dict(b='c')))
        self.assertEquals(util.dict_get_path(d, 'a.wrapped.b'), 'c')
        self.assertEquals(util.dict_get_path(d, 'a.nonexistent', 'fallback'), 'fallback')


class TestDictSetPath(unittest.TestCase):

    def test_setting_simple_unnested(self):
        d = dict(foo='bar')
        util.dict_set_path(d, 'foo', 'baz')
        self.assertEquals(d, dict(foo='baz'))

    def test_setting_nested(self):
        d = dict(a=dict(b='c'))
        util.dict_set_path(d, 'a.b', 'd')
        self.assertEquals(d, dict(a=dict(b='d')))
        util.dict_set_path(d, ('a', 'b'), 'e')
        self.assertEquals(d, dict(a=dict(b='e')))

    def test_setting_nonexistent(self):
        d = dict(a='b')
        util.dict_set_path(d, 'b.c', 'd')
        self.assertEquals(d, dict(a='b', b=dict(c='d')))

    def test_setting_attribute(self):

        class Bar(object):
            def __init__(self, wrapped):
                self.wrapped = wrapped

        d = dict(a=Bar(dict(b=Bar('c'))))
        util.dict_set_path(d, 'a.wrapped.b.wrapped', 'not c')
        self.assertEquals(util.dict_get_path(d, 'a.wrapped.b.wrapped'), 'not c')
        util.dict_set_path(d, 'a.wrapped', 'not a bar')
        self.assertEquals(util.dict_get_path(d, 'a.wrapped'), 'not a bar')

        self.assertRaises(AttributeError, util.dict_set_path, d, 'a.no-such-attribute', 'whatever')

    def test_setdefault(self):
        d = dict(a=dict(b='c'))
        self.assertEquals(util.dict_setdefault_path(d, 'a.b', 'd'), 'c')
        self.assertEquals(d, dict(a=dict(b='c')))
        self.assertEquals(util.dict_setdefault_path(d, 'a.c', 'd'), 'd')
        self.assertEquals(d, dict(a=dict(b='c', c='d')))

    def test_setdefault_through_attributes(self):
        class Bar(object):
            def __init__(self, wrapped):
                self.wrapped = wrapped

        d = dict(a=Bar(dict(b='c')))
        self.assertEquals(util.dict_setdefault_path(d, 'a.wrapped.b', 'd'), 'c')
        self.assertEquals(util.dict_get_path(d, 'a.wrapped.b'), 'c')
        self.assertEquals(util.dict_setdefault_path(d, 'a.wrapped.c', 'd'), 'd')
        self.assertEquals(util.dict_get_path(d, 'a.wrapped.c'), 'd')


class TestDictRemovePath(unittest.TestCase):

    def test_removing_nonexisting(self):
        d = dict(foo='bar')
        util.dict_remove_path(d, 'bar.baz')
        self.assertEquals(d, dict(foo='bar'))

    def test_removing_unnested(self):
        d = dict(a='b', c='d')
        util.dict_remove_path(d, 'a')
        self.assertEquals(d, dict(c='d'))

    def test_removing_nested(self):
        d = dict(a=dict(b='c'), d='e')
        util.dict_remove_path(d, 'a.b')
        self.assertEquals(d, dict(a=dict(), d='e'))
        util.dict_remove_path(d, 'a')
        self.assertEquals(d, dict(d='e'))
        util.dict_remove_path(d, 'd')
        self.assertEquals(d, dict())

    def test_removing_nonexisting_without_ignore_missing(self):
        d = dict(foo='bar', bar=dict())
        self.assertRaises(KeyError, util.dict_remove_path, d, 'bar.baz', ignore_missing=False)
        self.assertRaises(KeyError, util.dict_remove_path, d, 'foo.bar.baz', ignore_missing=False)
        self.assertEquals(d, dict(foo='bar', bar=dict()))


class TestAttributeDict(unittest.TestCase):

    def test_setting_and_getting(self):
        a = util.AttributeDict(foo='bar')
        a['bar'] = 'baz'
        self.assertEquals(a.foo, 'bar')
        self.assertEquals(a['foo'], 'bar')
        self.assertEquals(a.bar, 'baz')
        self.assertEquals(a['bar'], 'baz')

    def test_setting_and_deleting(self):
        a = util.AttributeDict(foo='bar')
        a['bar'] = 'baz'
        del a.foo
        del a.bar
        self.assertEquals(a.keys(), [])


class TestOrderedDictionary(unittest.TestCase):

    def test_replace_first_key(self):
        d = util.OrderedDictionary([('foo', 1), ('bar', 2), ('baz', 3)])
        d.replace_key('foo', 'notfoo', 1)
        self.assertEquals(d.items(), [('notfoo', 1), ('bar', 2), ('baz', 3)])

    def test_replace_middle_key(self):
        d = util.OrderedDictionary([('foo', 1), ('bar', 2), ('baz', 3)])
        d.replace_key('bar', 'notbar', 2)
        self.assertEquals(d.items(), [('foo', 1), ('notbar', 2), ('baz', 3)])

    def test_replace_last_key(self):
        d = util.OrderedDictionary([('foo', 1), ('bar', 2)])
        d.replace_key('bar', 'notbar', 2)
        self.assertEquals(d.items(), [('foo', 1), ('notbar', 2)])

    def test_replace_only_key(self):
        d = util.OrderedDictionary([('foo', 1)])
        d.replace_key('foo', 'notfoo', 1)
        self.assertEquals(d.items(), [('notfoo', 1)])

    def test_replace_nonexisting_key(self):
        d = util.OrderedDictionary([('foo', 1), ('bar', 2)])

        self.assertRaises(KeyError, d.replace_key, 'nonexisting', 'whatever', 'value')


class TestDictIterator(unittest.TestCase):
    def test_iterate_dict(self):
        d = dict(foo=123, bar=456)
        result = set(util.dict_iterate_paths(d))
        self.assertEqual(set([('foo', 123), ('bar', 456)]), result)

    def test_nested_iterate_dict(self):
        d = dict(foo=123, bar=456, baz=dict(zip='zap', foobar=42))
        result = sorted(util.dict_iterate_paths(d))
        self.assertEqual(result, [('bar', 456), ('baz', {'foobar': 42, 'zip': 'zap'}), ('baz.foobar', 42), ('baz.zip', 'zap'), ('foo', 123)])


class TestInTrial(unittest.TestCase):
    def test_trial_detection(self):
        self.assertTrue(util.in_unittest(), 'Failed to detect that we are running trial')


class TestEnsureDate(unittest.TestCase):

    def test_already_a_date(self):
        today = datetime.date.today()
        self.assertEqual(today, util.ensure_date(today))

    def test_converting_a_datetime(self):
        now = datetime.datetime.now()
        today = datetime.date.today()
        self.assertEqual(today, util.ensure_date(now))

    def test_converting_unixtime(self):
        i = 1297162800
        date = datetime.date(2011, 2, 8)
        self.assertEqual(date, util.ensure_date(i))


class TestPullFromQueueAndProcessWithDependency(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()

        self.queue = defer.DeferredQueue()
        self.processed = defer.DeferredQueue()

        pipeline_dependency = util.AttributeDict(wait_for_resource=lambda: defer.succeed(self.collector))
        stub_dependency_manager = util.AttributeDict(add_dependency=lambda x,y: pipeline_dependency)
        self.runtime_environment.dependency_manager = stub_dependency_manager

    def collector(self, baton):
        self.processed.put(baton)

    @defer.inlineCallbacks
    def test_simple_processing(self):
        puller = util.PullFromQueueAndProcessWithDependency(self.queue, 'pipeline.a_pipeline_name')
        puller.configure(self.runtime_environment)

        puller.startService()

        yield self.queue.put('a baton')
        baton = yield self.processed.get()
        self.assertEquals(baton, 'a baton')

        puller.stopService()

    def test_restart_without_duplicates_during_waiting_for_queue(self):
        puller = util.PullFromQueueAndProcessWithDependency(self.queue, 'pipeline.a_pipeline_name')

        @defer.inlineCallbacks
        def collector(baton):
            self.processed.put(baton)
            yield util.wait(0)

        self.collector = collector

        puller.configure(self.runtime_environment)

        puller.startService()

        puller.stopService()
        puller.startService()

        self.queue.put('1')
        self.queue.put('2')
        self.queue.put('3')

        # there should only be one processed baton
        self.assertEquals(self.processed.pending, ['1'])

        puller.stopService()

    @defer.inlineCallbacks
    def test_restart_without_duplicates_during_processing(self):
        puller = util.PullFromQueueAndProcessWithDependency(self.queue, 'pipeline.a_pipeline_name')

        @defer.inlineCallbacks
        def collector(baton):
            self.processed.put(baton)
            yield util.wait(0)

        self.collector = collector

        puller.configure(self.runtime_environment)

        puller.startService()

        self.queue.put('1')
        self.queue.put('2')
        self.queue.put('3')

        baton = yield self.processed.get()
        self.assertEquals(baton, '1')
        # the puller is now waiting on the collector sleep

        puller.stopService()
        puller.startService()

        yield util.wait(0)

        self.assertEquals(self.processed.pending, ['2'])

        puller.stopService()


class TestBatonJSONEncoder(unittest.TestCase):

    def test_encoding_unserializable(self):
        encoder = util.BatonJSONEncoder()
        obj = object()
        data = dict(object=obj, foo=[1,2,3])

        encoded = encoder.encode(data)
        # the object isn't json-serializable and should be repr'ed.
        self.assertEquals(encoded, '{"foo": [1, 2, 3], "object": "%r"}' % obj)


class TestFailAfterDelay(unittest.TestCase):

    @defer.inlineCallbacks
    def test_fail_after_delay(self):
        e = Exception()
        d = util.fail_after_delay(0, e)
        self.assertFalse(d.called)
        yield util.wait(0)
        self.assertTrue(d.called)

        try:
            yield d
            self.fail('expected errback')
        except Exception, actual_exception:
            self.assertTrue(actual_exception is e)


class TestWaitForFirst(unittest.TestCase):

    @defer.inlineCallbacks
    def test_wait_for_first(self):
        result = yield util.wait_for_first([defer.Deferred(), defer.succeed(42)])
        self.assertEquals(result, 42)

    @defer.inlineCallbacks
    def test_handling_failure(self):
        e = Exception()
        try:
            yield util.wait_for_first([defer.Deferred(), defer.fail(e)])
            self.fail('expected failure')
        except defer.FirstError, fe:
            self.assertTrue(fe.subFailure.value is e)

    @defer.inlineCallbacks
    def test_first_of_already_callbacked_deferreds(self):
        result = yield util.wait_for_first([defer.succeed(42), defer.fail(Exception())])
        self.assertEquals(result, 42)
        try:
            yield util.wait_for_first([defer.fail(Exception()), defer.succeed(42)])
            self.fail('expected failure')
        except defer.FirstError:
            pass


__doctests__ = [util]
