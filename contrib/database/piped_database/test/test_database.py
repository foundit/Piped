# Copyright (c) 2010-2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Test the database provider --- it should handle failures
gracefully and notify its customers upon connection, disconnections,
etc. """
import mock
import sqlalchemy as sa
from twisted.application import service
from twisted.internet import defer, reactor
from twisted.trial import unittest
from twisted.python import util as twisted_util, failure

from piped import conf, util, dependencies, processing
from piped_database import db, test as test_package
from piped_database import providers as database_provider


class FakeError(sa.exc.OperationalError):
    def __init__(self):
        pass

    def __str__(self):
        return 'FakeError'
    __repr__ = __str__


class EngineProviderTest(unittest.TestCase):
    timeout = 20
    configuration_manager = None

    def setUp(self):
        self.events = defer.DeferredQueue()
        self.mocker = mock.MagicMock()

    def make_engine_manager(self, profile_name='test_profile', **kwargs):
        kwargs.setdefault('engine', dict(url='sqlite://'))

        engine_manager = db.EngineManager(profile_name, **kwargs)

        # Only wait a reactor-iteration when testing. (There's an
        # assertion that this is non-zero, as that's not something you
        # want outside of tests, so we have to set it after making the
        # manager)
        engine_manager.ping_interval = 0
        engine_manager.retry_interval = 0

        engine_manager.on_connection_established += lambda engine: self.events.put(('connected', engine))
        engine_manager.on_connection_lost += lambda reason: self.events.put(('lost', reason))
        engine_manager.on_connection_failed += lambda reason: self.events.put(('failed', reason))

        return engine_manager

    @defer.inlineCallbacks
    def test_connect_event_called(self):
        """ Check that the right events are called when connections
        are established and lost. """
        engine_manager = self.make_engine_manager()

        engine_manager.engine = mocked_engine = self.mocker.engine
        mocked_connection = self.mocker.connection
        mocked_engine.connect.return_value=mocked_connection

        engine_manager.startService()

        event = yield self.events.get()
        self.assertEquals(event, ('connected', mocked_engine))

        engine_manager.stopService()
        yield util.wait(0)

        self.failIf(self.events.size > 0)

        self.assertEquals(self.mocker.mock_calls, [
                mock.call.engine.connect(),
                mock.call.connection.execute("SELECT 'ping'"),
                mock.call.connection.close(),
                mock.call.engine.dispose()
        ])

    @defer.inlineCallbacks
    def test_handling_connect_error(self):
        """ If an error occurs on connect, another attempt should be made. """
        engine_manager = self.make_engine_manager()
        engine_manager.engine = mocked_engine = self.mocker.engine
        mocked_connection = self.mocker.connection
        fake_error = FakeError()
        mocked_engine.connect.side_effect=util.get_callable_with_different_side_effects([fake_error, mocked_connection])

        engine_manager.startService()
        event = yield self.events.get()
        self.assertEquals(event[0], 'failed')
        self.assertTrue(event[1].value is fake_error)

        event = yield self.events.get()
        self.assertEquals(event, ('connected', mocked_engine))

        yield util.wait(0)
        engine_manager.stopService()
        yield util.wait(0)

        self.failIf(self.events.size > 0)
        self.assertEquals(self.mocker.mock_calls, [
            mock.call.engine.connect(),
            mock.call.engine.dispose(), # The first connect fails, so the engine is disposed.
            mock.call.engine.connect(),
            mock.call.connection(),
            mock.call.connection().execute("SELECT 'ping'"),
            mock.call.connection().close(),
            mock.call.engine.dispose(), # and disposed when the service stops.
            mock.call.engine.dispose() # and disposed when the service stops.
        ])

    @defer.inlineCallbacks
    def test_handling_ping_error_then_reconnecting(self):
        engine_manager = self.make_engine_manager()
        engine_manager.engine = mocked_engine = self.mocker.engine
        mocked_connection = self.mocker.connection
        fake_error = FakeError()
        mocked_engine.connect.side_effect=lambda: mocked_connection
        mocked_connection.execute.side_effect=util.get_callable_with_different_side_effects([None, fake_error, lambda *a, **kw: reactor.callFromThread(reactor.callLater, 0, engine_manager.stopService)])

        engine_manager.startService()

        self.assertEquals((yield self.events.get()), ('connected', mocked_engine))

        event = yield self.events.get()
        self.assertEquals(event[0], 'lost')
        self.assertTrue(event[1].value is fake_error)

        self.assertEquals((yield self.events.get()), ('connected', mocked_engine))

        self.failIf(self.events.size > 0)
        yield util.wait(0)

        self.assertEquals(self.mocker.mock_calls, [
            # First ping succeeds.
            mock.call.engine.connect(),
            mock.call.connection.execute("SELECT 'ping'"),
            mock.call.connection.close(),

            # This one fails. The engine is disposed as a result.
            mock.call.engine.connect(),
            mock.call.connection.execute("SELECT 'ping'"),
            mock.call.connection.close(),
            mock.call.engine.dispose(),

            # After this ping we stop the service.
            mock.call.engine.connect(),
            mock.call.connection.execute("SELECT 'ping'"),
            mock.call.connection.close(),
            mock.call.engine.dispose()
        ])
