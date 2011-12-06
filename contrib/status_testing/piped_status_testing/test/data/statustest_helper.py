# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from piped_status_testing import statustest, util
from twisted.internet import reactor, defer


""" This module contains statustests used only for internal testing of piped_status_testing. """

class HelperStatusTest(statustest.StatusTestCase):
    """ Most of these tests are just about setting up a namespace, then raising
    an exception in one way or another.
    """

    def setUp(self, **namespace):
        self.namespace = namespace

    def statustest_nothing(self):
        pass

    def statustest_fail_assert(self):
        self.assertEquals(1, 0)

    def statustest_fail_assert_namespace(self):
        some_value = 1
        other_value = 2
        self.assertEquals(some_value, other_value)

    def statustest_fail_assert_namespace_in(self):
        some_value = 1
        other_value = [2,3]
        self.assertIn(some_value, other_value)

    def statustest_raise_exception(self):
        raise Exception('raising an exception inside a statustest')

    # The unused but declared variables are inspected by unit tests
    # that inspect the locals of the callable raising the exception.
    def statustest_raise_exception_namespace(self):
        foo = 1
        raise Exception('raising an exception inside a statustest')

    def test_something(self):
        raise Exception('this method should not be run')

    def statustest_nested_functions(self):
        d = defer.Deferred()
        foo = 'foostring'
        def nested(result):
            bar = 'barstring'
            self.assertEqual(foo, bar)
        d.addCallback(nested)
        reactor.callLater(0, d.callback, None)
        return d

    def statustest_nested_raise(self):
        d = defer.Deferred()
        foo = 'foostring'
        def nested():
            bar = 'barstring'
            raise Exception('nested exception')
        d.addCallback(lambda x: nested())
        reactor.callLater(0, d.callback, None)
        return d

    def statustest_nested_raise_interesting_scoping(self):
        d = defer.Deferred()
        foo = 'foostring'
        def nested():
            bar = 'barstring'
            foo
            raise Exception('nested exception')
        d.addCallback(lambda x: nested())
        reactor.callLater(0, d.callback, None)
        return d

    @defer.inlineCallbacks
    def statustest_inlinecallbacks(self):
        def raiser(_=None):
            raise Exception('raised exception')
        foo = yield raiser()

    # Note, it's util.inlineCallbacks, not defer.inlineCallbacks!
    @util.inlineCallbacks
    def statustest_inlinecallbacks_util(self):
        def raiser(_=None):
            raise Exception('raised exception')
        foo = yield util.maybeDeferred(raiser)

    def statustest_should_skip(self):
        raise Exception('This test should have been skipped.')
    statustest_should_skip.skip = 'Skip this.'

    def statustest_todo(self):
        raise Exception('This test is intended to fail.')
    statustest_todo.todo = 'Not implemented yet.'

    def statustest_unexpected_success(self):
        pass
    statustest_unexpected_success.todo = 'This should result in an unexpected success.'
