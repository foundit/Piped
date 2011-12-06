# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# Copyright (c) 2001-2010 Twisted Matrix Laboratories.
#
# See LICENSE for details.
""" Status tests are tests that run inside a live process to do
system-/health-checks, etc --- possibly being re-run at arbitrary
intervals.

This module is heavily based on twisted.trial, but adapted to run
inside an already running reactor.
"""
import sys
import warnings

from twisted.internet import defer, reactor, utils
from twisted.python import failure, log as twisted_log
from twisted.trial import unittest, reporter, runner, itrial, util

from piped_status_testing import util as status_util


class StatusTestSuite(unittest.TestSuite):
    """ A TestSuite that runs its tests asynchronously. """

    def __init__(self, namespace=None, *a, **kw):
        super(StatusTestSuite, self).__init__(*a, **kw)
        self.namespace = namespace or dict()

    def __call__(self, namespace, result):
        return self.run(result)

    @defer.inlineCallbacks
    def run(self, result):
        for test in self._tests:
            if result.shouldStop:
                break
            yield test(self.namespace, result)
        defer.returnValue(result)


class StatusTestLoader(runner.TestLoader):
    """ A test loader that looks for functions/methods with the prefix
    ``statustest`` and creates :class:`StatusTestSuite` instances.
    """
    methodPrefix = 'statustest'
    modulePrefix = '' # what modules to look for statustests in. TODO: set to statustests_ ?
    test_suite_class = StatusTestSuite

    def __init__(self, namespace, *a, **kw):
        super(StatusTestLoader, self).__init__(*a, **kw)
        self.namespace = namespace
        self.suiteFactory = self.create_suite

    def create_suite(self, *a, **kw):
        return self.test_suite_class(self.namespace, *a, **kw)


class StreamAdapter(object):
    """ An adapter for streams that ensures that the twisted logging
    mechanism has an given context during each write.

    If the stream does not have an ``isatty`` attribute, this adapter provides
    one that returns ``False`` by default.
    """
    def __init__(self, stream=sys.stdout, system='status_test', isatty=False):
        """
        :param stream: A stream-like object.
        :param system: The system string to tell twisted.python.log to use.
        :param isatty: Used as the return value if the provided stream does not
            have an ``isatty`` attribute.
        """
        self._stream = stream

        self._system = system
        self._isatty = isatty

    def __getattr__(self, item):
        return getattr(self._stream, item)

    def write(self, data):
        twisted_log.callWithContext(dict(system=self._system), self._stream.write, data)

    def isatty(self):
        if hasattr(self._stream, 'isatty'):
            return self._stream.isatty()
        
        return self._isatty


class StatusReporter(reporter.TreeReporter):
    """ A reporter used for StatusTests.

    This reporter injects an adapted stream in order to work around
    http://twistedmatrix.com/trac/ticket/3067 since we run tests inside an
    already running process where logging always is configured.
    """

    def __init__(self, stream=sys.stdout, **kw):
        super(StatusReporter, self).__init__(stream=StreamAdapter(stream), **kw)


class ProcessorReporter(StatusReporter):
    """ Reporter that can pass test results using a processor. """

    def __init__(self, processor_dependency=None, *a, **kw):
        super(ProcessorReporter, self).__init__(*a, **kw)
        self.processor_dependency = processor_dependency
        self._in_processing = []

    def wait_for_result_processing(self):
        self._in_processing, ds = [], self._in_processing
        return defer.DeferredList(ds)

    @defer.inlineCallbacks
    def process_baton(self, baton):
        if not self.processor_dependency:
            return
        
        processor = yield self.processor_dependency.wait_for_resource()

        baton['reporter'] = self
        yield processor(baton)

    def addSuccess(self, test):
        super(ProcessorReporter, self).addSuccess(test)
        d = self.process_baton(dict(test=test, status='success'))
        self._in_processing.append(d)

    def addFailure(self, test, err):
        super(ProcessorReporter, self).addFailure(test, err)
        d = self.process_baton(dict(test=test, failure=err, status='failure'))
        self._in_processing.append(d)

    def addError(self, test, err):
        super(ProcessorReporter, self).addError(test, err)
        d = self.process_baton(dict(test=test, failure=err, status='error'))
        self._in_processing.append(d)

    def addSkip(self, test, err):
        super(ProcessorReporter, self).addSkip(test, err)
        d = self.process_baton(dict(test=test, failure=err, status='skipped'))
        self._in_processing.append(d)

    def addExpectedFailure(self, test, err, todo):
        super(ProcessorReporter, self).addExpectedFailure(test, err, todo)
        d = self.process_baton(dict(test=test, failure=err, status='expected_failure'))
        self._in_processing.append(d)

    def addUnexpectedSuccess(self, test, todo):
        super(ProcessorReporter, self).addUnexpectedSuccess(test, todo)
        d = self.process_baton(dict(test=test, todo=todo, status='todone'))
        self._in_processing.append(d)


class _MethodWrapper(object):
    """ A wrapper used to call a method with some injected keyword arguments. """

    def __init__(self, method, namespace):
        """
        :param method: The method to call
        :param namespace: A dict of injected keyword arguments.
        """
        self.namespace = namespace
        self.method = method

    def __call__(self, *a, **kw):
        kw.update(self.namespace)
        return self.method(*a, **kw)


class StatusTestCase(unittest.TestCase):
    """ An asynchronous TestCase that can be run inside a running reactor. """

    def _cleanUp(self, result):
        # Difference from unittest.TestCase: we dont call util._Janitor(self, result).postCaseCleanup(), which would mess up the reactor
        # XXX: This has the side-effect that we cannot fail test cases due to "dirty reactor state" etc.
        for error in self._observer.getErrors():
            result.addError(self, error)
            self._passed = False
        self.flushLoggedErrors()
        self._removeObserver()
        if self._passed:
            result.addSuccess(self)

    def _classCleanUp(self, result):
        # Difference from unittest.TestCase: skip calling util._Janitor(self, result).postClassCleanup(), which would mess up the reactor
        pass

    @defer.inlineCallbacks
    def run(self, namespace, result):
        """
        Run the test case, storing the results in C{result}.

        First runs C{setUp} on self, then runs the test method (defined in the
        constructor), then runs C{tearDown}. Any of these may return
        L{Deferred}s. After they complete, does some reactor cleanup.

        @param result: A L{TestResult} object.
        """
        # Difference from unittest.TestCase: we run in an asynchronous environment, so we yield instead of _wait
        #   We also inject the namespace as keyword arguments to the setUp method,
        #   and don't collect warnings.
        setattr(self, 'setUp', _MethodWrapper(getattr(self, 'setUp'), namespace))

        new_result = itrial.IReporter(result, None)
        if new_result is None:
            result = unittest.PyUnitResultAdapter(result)
        else:
            result = new_result
        self._timedOut = False
        result.startTest(self)
        if self.getSkip(): # don't run test methods that are marked as .skip
            result.addSkip(self, self.getSkip())
            result.stopTest(self)
            return
        self._observer = unittest._logObserver

        @defer.inlineCallbacks
        def runThunk():
            self._passed = False
            self._deprecateReactor(reactor)
            try:
                d = self.deferSetUp(None, result)
                try:
                    yield d
                finally:
                    self._cleanUp(result)
                    self._classCleanUp(result)
            finally:
                self._undeprecateReactor(reactor)

        yield runThunk()

        result.stopTest(self)

    def _run(self, methodName, result):
        # Difference from unittest.TestCase: we use maybe_deferred_with_noncleaning_failure in order to avoid having
        #                                    t.i.defer mangle our locals and globals
        timeout = self.getTimeout()
        def onTimeout(d):
            e = defer.TimeoutError("%r (%s) still running at %s secs"
                % (self, methodName, timeout))
            f = failure.Failure(e)
            # try to errback the deferred that the test returns (for no gorram
            # reason) (see issue1005 and test_errorPropagation in
            # test_deferred)
            try:
                d.errback(f)
            except defer.AlreadyCalledError:
                # if the deferred has been called already but the *back chain
                # is still unfinished, crash the reactor and report timeout
                # error ourself.
                # reactor.crash() # TODO: decide what to do wrt timeouts -- Njal
                self._timedOut = True # see self._wait
                todo = self.getTodo()
                if todo is not None and todo.expected(f):
                    result.addExpectedFailure(self, f, todo)
                else:
                    result.addError(self, f)
        onTimeout = utils.suppressWarnings(
            onTimeout, util.suppress(category=DeprecationWarning))
        method = getattr(self, methodName)
        d = status_util.maybe_deferred_with_noncleaning_failure(utils.runWithWarningsSuppressed,
                                self.getSuppress(), method)
        call = reactor.callLater(timeout, onTimeout, d)
        d.addBoth(lambda x : call.active() and call.cancel() or x)
        return d
