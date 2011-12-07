# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
""" This module is heavily based on twisted.trial, but adapted to run
    inside an already running reactor.
"""
import sys

import warnings
from twisted.internet import defer
from twisted.python import failure
from twisted.trial import unittest

from piped import util


@defer.inlineCallbacks
def _deferred_collectWarnings(observeWarning, f, *args, **kwargs):
    """
    Call C{f} with C{args} positional arguments and C{kwargs} keyword arguments
    and collect all warnings which are emitted as a result in a list.

    @param observeWarning: A callable which will be invoked with a L{unittest._Warning}
        instance each time a warning is emitted.

    @return: The return value of C{f(*args, **kwargs)}.
    """
    def showWarning(message, category, filename, lineno, file=None, line=None):
        assert isinstance(message, Warning)
        observeWarning(unittest._Warning(
                message.args[0], category, filename, lineno))

    # Disable the per-module cache for every module otherwise if the warning
    # which the caller is expecting us to collect was already emitted it won't
    # be re-emitted by the call to f which happens below.
    for v in sys.modules.itervalues():
        if v is not None:
            try:
                v.__warningregistry__ = None
            except:
                # Don't specify a particular exception type to handle in case
                # some wacky object raises some wacky exception in response to
                # the setattr attempt.
                pass

    origFilters = warnings.filters[:]
    origShow = warnings.showwarning
    warnings.simplefilter('always')
    try:
        warnings.showwarning = showWarning
        result = yield f(*args, **kwargs)
    finally:
        warnings.filters[:] = origFilters
        warnings.showwarning = origShow
    defer.returnValue(result)

def maybe_deferred_with_noncleaning_failure(f, *args, **kwargs):
    d = None
    try:
        result = f(*args, **kwargs)
    except:
        return defer.fail(util.NonCleaningFailure())
    else:
        if isinstance(result, defer.Deferred):
            def replacer(failure):
                f = util.NonCleaningFailure(failure.type, failure.value)
                f.__dict__ = failure.__dict__
                return f
            result.addErrback(replacer)
            return result
        elif isinstance(result, failure.Failure):
            f = util.NonCleaningFailure()
            f.__dict__ = result.__dict__
            return defer.fail(f)
        else:
            return defer.succeed(result)
    return d

maybeDeferred = maybe_deferred_with_noncleaning_failure

def _inlineCallbacks(result, g, deferred):
    """
    See L{inlineCallbacks}.
    """
    # This function is complicated by the need to prevent unbounded recursion
    # arising from repeatedly yielding immediately ready deferreds.  This while
    # loop and the waiting variable solve that by manually unfolding the
    # recursion.

    waiting = [True, # waiting for result?
               None] # result

    while 1:
        try:
            # Send the last result back as the result of the yield expression.
            if isinstance(result, failure.Failure):
                result = result.throwExceptionIntoGenerator(g)
            else:
                result = g.send(result)
        except StopIteration:
            # fell off the end, or "return" statement
            deferred.callback(None)
            return deferred
        except defer._DefGen_Return, e:
            # returnValue call
            deferred.callback(e.value)
            return deferred
        except:
            f = util.NonCleaningFailure()
            deferred.errback(f)
            return deferred

        if isinstance(result, defer.Deferred):
            # a deferred was yielded, get the result.
            def gotResult(r):
                if waiting[0]:
                    waiting[0] = False
                    waiting[1] = r
                else:
                    _inlineCallbacks(r, g, deferred)

            result.addBoth(gotResult)
            if waiting[0]:
                # Haven't called back yet, set flag so that we get reinvoked
                # and return from the loop
                waiting[0] = False
                return deferred

            result = waiting[1]
            # Reset waiting to initial values for next loop.  gotResult uses
            # waiting, but this isn't a problem because gotResult is only
            # executed once, and if it hasn't been executed yet, the return
            # branch above would have been taken.


            waiting[0] = True
            waiting[1] = None


    return deferred

def inlineCallbacks(f):
    """
    WARNING: this function will not work in Python 2.4 and earlier!

    inlineCallbacks helps you write Deferred-using code that looks like a
    regular sequential function. This function uses features of Python 2.5
    generators.  If you need to be compatible with Python 2.4 or before, use
    the L{deferredGenerator} function instead, which accomplishes the same
    thing, but with somewhat more boilerplate.  For example::

        def thingummy():
            thing = yield makeSomeRequestResultingInDeferred()
            print thing #the result! hoorj!
        thingummy = inlineCallbacks(thingummy)

    When you call anything that results in a Deferred, you can simply yield it;
    your generator will automatically be resumed when the Deferred's result is
    available. The generator will be sent the result of the Deferred with the
    'send' method on generators, or if the result was a failure, 'throw'.

    Your inlineCallbacks-enabled generator will return a Deferred object, which
    will result in the return value of the generator (or will fail with a
    failure object if your generator raises an unhandled exception). Note that
    you can't use 'return result' to return a value; use 'returnValue(result)'
    instead. Falling off the end of the generator, or simply using 'return'
    will cause the Deferred to have a result of None.

    The Deferred returned from your deferred generator may errback if your
    generator raised an exception::

        def thingummy():
            thing = yield makeSomeRequestResultingInDeferred()
            if thing == 'I love Twisted':
                # will become the result of the Deferred
                returnValue('TWISTED IS GREAT!')
            else:
                # will trigger an errback
                raise Exception('DESTROY ALL LIFE')
        thingummy = inlineCallbacks(thingummy)
    """
    def unwindGenerator(*args, **kwargs):
        return _inlineCallbacks(None, f(*args, **kwargs), defer.Deferred())
    return defer.mergeFunctionMetadata(f, unwindGenerator)