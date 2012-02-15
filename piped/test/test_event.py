# Copyright (c) 2010-2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.trial import unittest

from piped import event, util, exceptions


class TestEvent(unittest.TestCase):

    def test_a_callback_can_remove_itself(self):
        e = event.Event()
        l = list()

        def append_just_once():
            l.append(1)
            e.unhandle(append_just_once)

        def keep_appending():
            l.append(2)

        e += append_just_once
        e += keep_appending

        # the event should now contain these callbacks
        self.assertIn(append_just_once, e)
        self.assertIn(keep_appending, e)

        self.assertEquals(len(e), 2)

        e()
        self.assertEquals(l, [1, 2])

        # At this point, the first callback should have removed itself.
        e()
        # So we should not get an additional 1.
        self.assertEquals(l, [1, 2, 2])

    def test_removing_an_nonexistent_callback_raises(self):
        e = event.Event()
        self.assertRaises(ValueError, e.unhandle,  lambda: None)

    @defer.inlineCallbacks
    def test_wait_until_fired(self):
        e = event.Event()

        d = e.wait_until_fired()
        self.assertFalse(d.called)

        # the deferred should be callbacked when the event is fired.
        e('foo', bar='baz')
        self.assertTrue(d.called)

        args, kwargs = yield d
        self.assertEquals(args, ('foo',))
        self.assertEquals(kwargs, dict(bar='baz'))

    @defer.inlineCallbacks
    def test_wait_until_fired_timeout(self):
        e = event.Event()

        d = e.wait_until_fired(timeout=0)
        self.assertFalse(d.called)

        # the deferred should be errbacked if the timeout is reached:
        yield util.wait(0)

        self.assertTrue(d.called)

        try:
            yield d
            self.fail('TimeoutError not raised.')
        except exceptions.TimeoutError as te:
            pass

    @defer.inlineCallbacks
    def test_delayed_call_is_cancelled(self):
        e = event.Event()

        d = e.wait_until_fired(timeout=1)

        e('foo')

        foo = yield d
        self.assertEquals(foo, (('foo',), {}))

        # since we called wait_until_fired with a timeout, a timeout was created, but the event
        # was fired before the timeout was reached. In this case, the timeout delayed call should
        # have been cancelled. If it is not cancelled, this test will fail with a
        # DirtyReactorAggregateError.


__doctests__ = [event]
