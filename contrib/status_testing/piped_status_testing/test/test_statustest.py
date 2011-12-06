# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from StringIO import StringIO

import yaml
from twisted.internet import defer
from twisted.python import reflect
from twisted.trial import unittest

from piped_status_testing import statustest


# Heh. :-)
class StatusTestTest(unittest.TestCase):

    def setUp(self):
        self.namespace = dict()
        self.loader = statustest.StatusTestLoader(self.namespace)

        self.package_name = __name__.rsplit('.', 1)[0]
        self.default_test_namespace = reflect.namedAny(self.package_name+'.data')
        self.status_test_class_name = self.package_name + '.data.statustest_helper.HelperStatusTest'

    def get_globals_and_locals_from_frame_in_result(self, result, frame=-1):
        _, failure = result

        # try to find the locals and globals in the failure instance
        last_frame = failure.frames[frame]
        locals = dict(last_frame[-2])
        globals = dict(last_frame[-1])

        if not locals and not globals and failure.tb:
            # but they might not have been captured, so we have to check the traceback
            tracebacks = [failure.tb]

            while tracebacks[-1] and tracebacks[-1].tb_next:
                tracebacks.append(tracebacks[-1].tb_next)

            tb = tracebacks[frame]

            locals = tb.tb_frame.f_locals
            globals = tb.tb_frame.f_globals

        return globals, locals

    def get_reporter_with_suite_run(self):
        suite = self.loader.loadAnything(self.default_test_namespace, recurse=True)
        return suite.run(statustest.StatusReporter(stream=StringIO()))

    def get_result_by_test_name(self, list_of_tests, test_name):
        for test in list_of_tests:
            if test_name in str(test):
                return test

    def assertLocalsEqual(self, result, expected_locals, frame=-1):
        _, locals = self.get_globals_and_locals_from_frame_in_result(result, frame=frame)
        for key, value in expected_locals.items():
            self.assertEquals(locals[key], value)

    @defer.inlineCallbacks
    def test_simple_run(self):
        """ Test that at least one statustest gets executed. """
        reporter = yield self.get_reporter_with_suite_run()
        self.assertTrue(reporter.testsRun > 0)

    @defer.inlineCallbacks
    def test_correct_failure_and_errors(self):
        """ Test that the correct failures/exceptions gets propagated to the reporter. """
        reporter = yield self.get_reporter_with_suite_run()

        fail_assert_result = self.get_result_by_test_name(reporter.failures, 'statustest_fail_assert')
        self.assertTrue('failure', fail_assert_result[0])
        self.assertEquals(fail_assert_result[1].type, unittest.FailTest)

        raise_exception_result = self.get_result_by_test_name(reporter.errors, 'statustest_raise_exception')
        self.assertTrue('error', raise_exception_result[0])
        self.assertEquals(raise_exception_result[1].type, Exception)
        self.assertEquals(raise_exception_result[1].value.args[0], 'raising an exception inside a statustest')

    @defer.inlineCallbacks
    def test_correct_traceback(self):
        """ Test that the traceback (locals/globals and their contents) are as expected. """
        reporter = yield self.get_reporter_with_suite_run()

        expected_locals_for_test_failures = dict(
            statustest_fail_assert_namespace=dict(some_value=1, other_value=2),
            statustest_fail_assert_namespace_in=dict(some_value=1, other_value=[2,3]),
            statustest_nested_functions=dict(foo='foostring', bar='barstring')
        )

        for test_name, expected_locals in expected_locals_for_test_failures.items():
            result = self.get_result_by_test_name(reporter.failures, test_name)
            self.assertLocalsEqual(result, expected_locals, frame=-2)

        expected_locals_for_test_errors = dict(
            statustest_raise_exception_namespace=dict(foo=1),
        )

        for test_name, expected_locals in expected_locals_for_test_errors.items():
            result = self.get_result_by_test_name(reporter.errors, test_name)
            self.assertLocalsEqual(result, expected_locals)

    @defer.inlineCallbacks
    def test_correct_namespaces(self):
        """ Test that the namespace behaves as expected. """
        reporter = yield self.get_reporter_with_suite_run()

        result = self.get_result_by_test_name(reporter.errors, 'statustest_nested_raise')
        globals, locals = self.get_globals_and_locals_from_frame_in_result(result)

        self.assertNotIn('foo', locals)
        self.assertNotIn('foo', globals)
        self.assertEquals(locals['bar'], 'barstring')

        result = self.get_result_by_test_name(reporter.errors, 'statustest_nested_raise_interesting_scoping')
        self.assertLocalsEqual(result, dict(foo='foostring', bar='barstring'))

    @defer.inlineCallbacks
    def test_inserted_namespace(self):
        """ Test that the inserted namespace is working. """
        obj = object()
        self.namespace['my_namespace_key'] = 414
        self.namespace['secret_object'] = obj
        reporter = yield self.get_reporter_with_suite_run()

        result = self.get_result_by_test_name(reporter.errors, 'statustest_raise_exception')
        globs, locs = self.get_globals_and_locals_from_frame_in_result(result)
        self.assertEquals(locs['self'].namespace, dict(my_namespace_key=414, secret_object=obj))

    @defer.inlineCallbacks
    def test_inlinecallbacks(self):
        """ Test that our inlineCallbacks works as expected. """
        reporter = yield self.get_reporter_with_suite_run()

        self.assertLocalsEqual(self.get_result_by_test_name(reporter.errors, 'statustest_inlinecallbacks_util'), dict(_=None))
        # but locals are lost when using the default @defer.inlineCallbacks:
        self.assertLocalsEqual(self.get_result_by_test_name(reporter.errors, 'statustest_inlinecallbacks'), dict())

    @defer.inlineCallbacks
    def test_should_skip(self):
        """ Test that a test is skipped. """
        reporter = yield self.get_reporter_with_suite_run()

        result = self.get_result_by_test_name(reporter.skips, 'statustest_should_skip')
        self.assertNotEquals(result, None)

    @defer.inlineCallbacks
    def test_todos(self):
        """ Test that a tests marked todo are run. """
        reporter = yield self.get_reporter_with_suite_run()

        result = self.get_result_by_test_name(reporter.expectedFailures, 'statustest_todo')
        self.assertNotEquals(result, None)

        result = self.get_result_by_test_name(reporter.unexpectedSuccesses, 'statustest_unexpected_success')
        self.assertNotEquals(result, None)
