# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Test the database provider --- it should handle failures
gracefully and notify its customers upon connection, disconnections,
etc. """
import mocker
import sqlalchemy as sa
from twisted.application import service
from twisted.internet import defer
from twisted.trial import unittest
from twisted.python import util as twisted_util, failure

from piped import conf, util, dependencies, processing
from piped_database import db, test as test_package
from piped_database import providers as database_provider


class DatabaseProviderTest(unittest.TestCase):
    timeout = 2
    configuration_manager = None

    def setUp(self):
        # only set up the configuration manager once per class:
        if not self.configuration_manager:
            self.configuration_manager = conf.ConfigurationManager()
            piped_conf_file = twisted_util.sibpath(test_package.__file__, 'conf.yml')
            self.configuration_manager.load_from_file(piped_conf_file)

        self.mocker = mocker.Mocker()

        database_configuration = self.configuration_manager.get('test_database')
        self.metadata_provider = db.DatabaseMetadataManager(database_configuration)

    def tearDown(self):
        self.mocker.restore()
        self.mocker.verify()

    def disable_logging(self):
        log = self.mocker.replace('piped.log')
        log.error(mocker.ANY)
        self.mocker.count(0, None)

    @defer.inlineCallbacks
    def test_connection_events_called(self):
        """ Check that the right events are called when connections
        are established and lost. """

        queue = defer.DeferredQueue()

        self.metadata_provider.on_connection_established += lambda *a: queue.put('established')
        self.metadata_provider.on_connection_lost += lambda *a: queue.put('lost')

        # Connect and check that the event was called.
        self.metadata_provider.connect()
        event_invoked = yield queue.get()
        self.assertEquals(event_invoked, 'established')

        # It should be idempotent.
        self.metadata_provider.connect()
        yield util.wait(0) # To give any unwanted event a chance to put something in the queue.
        self.failIf(queue.size > 0, "When already connected, an additional connect() should not invoke the established-event")

        # Disconnect and check that the event was called.
        self.metadata_provider.disconnect()

        event_invoked = yield queue.get()
        self.assertEquals(event_invoked, 'lost')

    def test_with_reflection_an_initial_connection_is_established_with_reflection(self):
        """ Check that a connection is established when
        reflecting. """
        self.metadata_provider.connect(reflect=True)
        self.assertEquals(self.metadata_provider.metadata.bind.pool.checkedin(), 1,
                          "A connection was not established when we expected it to")

    def test_with_reflection_an_initial_connection_is_established_without_reflection(self):
        """ Check that a connection is established also when not
        reflecting. """
        self.metadata_provider.connect(reflect=False)
        self.assertEquals(self.metadata_provider.metadata.bind.pool.checkedin(), 1,
                          "A connection was not established when we expected it to")

    @defer.inlineCallbacks
    def test_connected_event_not_called_upon_connection_error(self):
        """ If a connection cannot be established, the
        on_connection_established must not be invoked."""

        # Set the initial connection up to fail.
        fake_metadata = self.mocker.mock()
        fake_metadata.reflect()
        exception = sa.exc.SQLAlchemyError()
        self.mocker.throw(exception)

        self.metadata_provider.metadata_factory = lambda engine: fake_metadata

        # connect event should not be invoked, whereas connection_failed should.
        def fail(_):
            self.fail('Expected on_connection_established to NOT be called.')
        self.metadata_provider.on_connection_established += fail

        on_connection_failed = defer.Deferred()
        self.metadata_provider.on_connection_failed += on_connection_failed.callback

        self.disable_logging()
        self.mocker.replay()

        try:
            self.metadata_provider.connect()
            self.fail("Expected an SQLAlchemyError")
        except sa.exc.SQLAlchemyError:
            pass

        try:
            yield on_connection_failed
            self.fail("Expected to a failure")
        except sa.exc.SQLAlchemyError:
            pass

        self.mocker.verify()

    @defer.inlineCallbacks
    def test_reconnect(self):
        """ When configured to reconnect, the provider should
        reconnect and eventually call the connection established
        event. """
        queue = defer.DeferredQueue()
        # Hook up subscribers.
        self.metadata_provider.on_connection_established += lambda *a: queue.put('established')
        self.metadata_provider.on_connection_lost += lambda *a: queue.put('lost')

        # Reconnection logic expects an initial connection to have existed.
        self.metadata_provider.connect()
        event = yield queue.get()
        self.assertEquals(event, 'established')

        # This should call the connection lost events.
        self.metadata_provider.disconnect()
        event = yield queue.get()
        self.assertEquals(event, 'lost')

        self.metadata_provider.startService()
        # This should eventually result in a new connection.
        self.metadata_provider.keep_reconnecting_and_report()
        event = yield queue.get()
        self.assertEquals(event, 'established')

    @defer.inlineCallbacks
    def test_reconnect_survives_failure(self):
        """ Check that the provider survives reconnection failures and
        eventually succeeds. """
        queue = defer.DeferredQueue()
        # Hook up subscribers.
        self.metadata_provider.on_connection_established += lambda *a: queue.put('established')
        self.metadata_provider.on_connection_lost += lambda *a: queue.put('lost')

        # Reconnection logic expects an initial connection to have existed.
        self.metadata_provider.connect(reflect=False)
        self.metadata_provider.disconnect()

        event = yield queue.get()
        self.assertEquals(event, 'established')

        # Lose the connection.
        event = yield queue.get()
        self.assertEquals(event, 'lost')

        self.metadata_provider.reconnect_wait = 0.1 # ... skip the waiting.
        self.metadata_provider.running = True
        # Set the provider up to fail.
        self.metadata_provider = self.mocker.patch(self.metadata_provider)

        self.metadata_provider.test_connectivity()
        self.mocker.throw(sa.exc.SQLAlchemyError())
        self.metadata_provider.test_connectivity()
        self.mocker.passthrough()
        self.mocker.count(1, max=None)

        self.mocker.replay()

        # Start reconnecting
        d = self.metadata_provider.keep_reconnecting_and_report()
        # Check idempotency.
        d2 = self.metadata_provider.keep_reconnecting_and_report()
        self.assertEquals(d, d2)

        yield d
        # Make sure we are really connected when the reconnection-deferred has callbacked.
        self.metadata_provider.test_connectivity()

    @defer.inlineCallbacks
    def test_connect_to_nonexisting_host_times_out(self):
        """ Ensure that connection eventually times out. """
        # timeout is expected to be an integer, and 0 means no
        # timeout, so we'll have to spend a second if we want to keep
        # things simple.
        timeout = 1
        wrong_config = dict(user='foo', password='foo', host='127.0.0.2',
                            database_name='if_this_exists_it_is_my_own_fault_some_tests_fail',
                            timeout=timeout)

        self.disable_logging()
        self.mocker.replay()

        # Make a new provider, since the default one from setUp is correctly configured.
        self.metadata_provider = db.DatabaseMetadataManager(wrong_config)

        try:
            self.metadata_provider.connect()
            self.fail("The connect should fail")
        except sa.exc.SQLAlchemyError:
            pass
        # wait one reactor iteration, so the callFromThread error from the connection-attempt gets executed
        yield util.wait(0)

    def test_connect_with_existing_metadata(self):
        """ Test that we can provide our own metadata. """
        metadata = sa.MetaData()

        self.metadata_provider.connect(reflect=True, existing_metadata=metadata)
        # Check that reflection has happened:
        self.assertTrue(metadata.tables, "No tables were reflected")

        self.assertEquals(self.metadata_provider.metadata.bind.pool.checkedin(), 1,
                          "A connection was not established when we expected it to")


class TestDatabaseMetadataProvider(unittest.TestCase):
    timeout = 2

    def setUp(self):
        self.mocker = mocker.Mocker()
        self.provider = database_provider.DatabaseMetadataProvider()

        self.environment = processing.RuntimeEnvironment()
        self.environment.application = service.MultiService()
        self.environment.resource_manager = self.resource_manager = self.mocker.mock()

    def tearDown(self):
        self.mocker.restore()

    def disable_logging(self):
        log = self.mocker.replace('piped.log')
        log.error(mocker.ANY)
        self.mocker.count(0, None)

    def set_profiles(self, database_profiles):
        self.environment.configuration_manager.set('database.profiles', database_profiles)

    def test_all_profiles_are_provided(self):
        database_profiles = dict(foo={}, bar={})
        self.set_profiles(database_profiles)

        for profile_name in database_profiles:
            self.resource_manager.register('database.%s'%profile_name, provider=self.provider)

        self.mocker.replay()

        self.provider.configure(self.environment)

        self.mocker.restore()
        self.mocker.verify()

    @defer.inlineCallbacks
    def test_connection_event_propagates_to_the_consumer(self):
        provider = database_provider.DatabaseMetadataProvider()

        # Make the connect succeed without actually connecting.
        fake_manager = provider._manager_for_profile['fake_profile'] = self.mocker.patch(db.DatabaseMetadataManager(dict()))
        fake_manager.connect()
        self.mocker.call(lambda: fake_manager.on_connection_established(None))

        # Make sure that success results in an on_resource_ready on the dependency.
        d = defer.Deferred()
        resource_dependency = dependencies.ResourceDependency(provider='database.fake_profile')
        resource_dependency.on_resource_ready += d.callback
        resource_dependency.on_resource_lost += lambda reason: d.errback(reason)

        self.mocker.replay()

        provider.add_consumer(resource_dependency)

        result = yield d
        self.assertTrue(result is fake_manager)

        self.mocker.verify()

    @defer.inlineCallbacks
    def test_on_connection_failed_propagates_to_the_consumer(self):
        provider = database_provider.DatabaseMetadataProvider()

        # Make the connect succeed without actually connecting.
        fake_manager = provider._manager_for_profile['fake_profile'] = self.mocker.patch(db.DatabaseMetadataManager(dict()))
        fake_manager.connect()
        self.mocker.call(lambda: fake_manager.on_connection_failed(failure.Failure(sa.exc.SQLAlchemyError())))

        # Make sure that success results in an on_resource_ready on the dependency.
        d = defer.Deferred()
        resource_dependency = dependencies.ResourceDependency(provider='database.fake_profile')
        resource_dependency.on_resource_ready += d.callback
        resource_dependency.on_resource_lost += lambda reason: d.errback(reason)

        self.mocker.replay()

        provider.add_consumer(resource_dependency)

        try:
            yield d
            self.fail('Expected the connect call to result in an exception')
        except sa.exc.SQLAlchemyError:
            pass

        self.mocker.verify()


# Skip these tests if psycopg2 is not installed
# TODO: Do the tests with SQLite.
try:
    import psycopg2
except ImportError:
    DatabaseProviderTest.skip = 'Did not find psycopg2'
