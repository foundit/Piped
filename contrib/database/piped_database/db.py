# -*- test-case-name: piped.test.test_database -*-

# Copyright (c) 2010-2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import collections
import copy
import functools
import logging
import heapq
import urlparse
import warnings

import sqlalchemy as sa
import sqlalchemy.interfaces
from twisted.application import service
from twisted.internet import defer, reactor, threads
from twisted.python import failure, reflect

from piped import event, exceptions, util, service as piped_service


logger = logging.getLogger(__name__)


# We're extra happy with Postgres-stuff around, but don't require it.
try:
    import psycopg2
    from txpostgres import txpostgres
except ImportError:
    psycopg2 = None
    txpostgres = None


class DatabaseError(exceptions.PipedError):
    pass


class EngineManager(object, service.Service):

    def __init__(self, configuration, profile_name):
        self.configuration = configuration
        self.profile_name = profile_name
        configuration.setdefault('ping_interval', 10.0)
        configuration.setdefault('retry_interval', 5.0)

        self._fail_if_configuration_is_invalid(configuration)

        # We'll manipulate the configuration, so copy it.
        configuration['engine'] = copy.deepcopy(configuration['engine'])
        configuration['engine'].setdefault('proxy', ConnectionProxy(self))

        self.is_connected = False
        self.currently = util.create_deferred_state_watcher(self)

        self.engine = sa.engine_from_config(configuration['engine'], prefix='')
        self._bind_events()

        self.on_connection_established = event.Event()
        self.on_connection_lost = event.Event()
        self.on_connection_failed = event.Event()

    def _bind_events(self):
        for event, list_of_handlers in self.configuration.get('events', dict()).items():
            for handler_name in list_of_handlers:
                sa.event.listen(self.engine, event, reflect.namedAny(handler_name))

        for sql_string in self.configuration.get('checkout', []):
            sa.event.listen(self.engine, 'checkout', functools.partial(self.on_checkout, sql_string))

        for sql_string in self.configuration.get('checkin', []):
            sa.event.listen(self.engine, 'checkin', functools.partial(self.on_checkin, sql_string))

    def on_checkout(self, sql, dbapi_connection, connection_record, connection_proxy):
        try:
            dbapi_connection.cursor().execute(sql)
        except Exception:
            # The connection is in an unusable state, and this is communicated to the dependencies. Nothing
            # more we can do here.
            pass

    def on_checkin(self, sql, dbapi_connection, connection_record):
        try:
            if dbapi_connection:
                dbapi_connection.cursor().execute(sql)
        except Exception:
            pass

    @classmethod
    def _fail_if_configuration_is_invalid(cls, configuration):
        if not configuration.get('engine'):
            raise exceptions.ConfigurationError('no engine-configuration provided')
        if not configuration['engine'].get('url'):
            raise exceptions.ConfigurationError('missing database-URL')
        if not configuration['ping_interval']:
            detail = 'currently set to "%r"' % configuration['ping_interval']
            raise exceptions.ConfigurationError('Please specify a non-zero ping-interval', detail)

    def startService(self):
        if self.running:
            return

        service.Service.startService(self)
        self._connect_and_stay_connected()

    def stopService(self):
        if not self.running:
            return

        service.Service.stopService(self)
        if self._currently:
            self._currently.cancel()

    @defer.inlineCallbacks
    def _connect_and_stay_connected(self):
        while self.running:
            try:
                # Connect and ping. The engine is a pool, so we're not
                # really establishing new connections all the time.
                logger.info('Attempting to connect to "{0}"'.format(self.profile_name))
                yield self.currently(threads.deferToThread(self._test_connectivity, self.engine))
                logger.info('Connected to "{0}"'.format(self.profile_name))

                if not self.is_connected:
                    self.on_connection_established(self.engine)

                self.is_connected = True

                try:
                    while self.running:
                        yield self.currently(util.wait(self.configuration['ping_interval']))
                        yield self.currently(threads.deferToThread(self._test_connectivity, self.engine))
                except defer.CancelledError:
                    pass

            except Exception as e:
                logger.error('Error with engine "{0}": {1}'.format(self.profile_name, e.message))
                failure_ = failure.Failure()
                if self.is_connected:
                    logger.error('Lost connection to "{0}"'.format(self.profile_name))
                    self.on_connection_lost(failure_)

                self.is_connected = False
                self.on_connection_failed(failure_)

                yield self.currently(util.wait(self.configuration['retry_interval']))

            except defer.CancelledError:
                pass

            finally:
                self.engine.dispose()

    def _test_connectivity(self, engine):
        # This is actually checking in and out of an engine pool. We're not actually
        # establishing new connections all the time.
        connection = engine.connect()
        try:
            connection.execute("SELECT 'ping'")
        finally:
            connection.close()


class ConnectionProxy(sqlalchemy.interfaces.ConnectionProxy):
    """ Proxy that intercepts executed cursors and relays
    `OperationalError`s to the manager. """

    intercepted_exceptions = (sa.exc.OperationalError, )

    def __init__(self, manager):
        self.manager = manager

    # See SQLAlchemy's documentation on this one.
    def cursor_execute(self, execute, cursor, statement, parameters, context, executemany):
        try:
            return execute(cursor, statement, parameters, context)
        except self.intercepted_exceptions:
            reactor.callFromThread(self.manager.on_connection_failed, failure.Failure())
            raise


class PostgresListener(piped_service.PipedService):
    """Service that deals with PostgreSQL NOTIFY-triggers, with
    support for watching transaction snapshot visibility tresholds.

    This is useful when you need to react quickly to changes in the database.

    Watching transaction id (hence "txid") thresholds can help when
    implementing sync strategies. By ensuring that notifications of
    changes are sent in monotonically increasing txid order, we can
    now that we have everything up to txid if an error occurs, even
    when transactions commit out of order.

    Note that this approach assumes that you will not have a lot of
    long-running transactions, as those will temporarily block
    notifications.

    This service is not intended to be used directly to issue
    arbitrary SQL. Use the EngineProvider if you need more data from
    the database on notifications.
    """

    def __init__(self, configuration, profile_name):
        super(PostgresListener, self).__init__()
        if not txpostgres and psycopg2:
            raise exceptions.ConfigurationError('cannot use PostgresListener without psycopg2 and txpostgres')

        self.configuration = configuration
        self.profile_name = profile_name
        self._lock = defer.DeferredLock()
        self._connection = None
        self._listeners = collections.defaultdict(set)

        self._txid_queue = []
        self._currently_pinging = None
        self.currently_pinging = util.create_deferred_state_watcher(self, '_currently_pinging')

        self.is_connected = False
        self.on_connection_established = event.Event()
        self.on_connection_lost = event.Event()
        self.on_connection_failed = event.Event()

        configuration.setdefault('ping_interval', 10.0)
        configuration.setdefault('retry_interval', 5.0)
        configuration.setdefault('txid_poll_interval', .1)      

    def _connect(self):
        url = urlparse.urlparse(self.configuration['engine']['url'])
        dsn = dict(
            database=url.path.strip('/'),
            password=url.password,
            user=url.username,
            host=url.hostname,
            port=url.port
        )

        self._connection = txpostgres.Connection()
        return self._connection.connect(**dsn)

    def _notify(self, notification):
        for queue in self._listeners[notification.channel]:
            queue.put(notification)

    @defer.inlineCallbacks
    def _run_exclusively(self, func, *a, **kw):
        yield self._lock.acquire()
        try:
            result = yield self.cancellable(func(*a, **kw))
            defer.returnValue(result)
        finally:
            self._lock.release()

    @defer.inlineCallbacks
    def run(self):
        try:
            while self.running:
                try:
                    logger.info('PostgresListener attempting to connect to "{0}"'.format(self.profile_name))
                    yield self._run_exclusively(self._connect)
                    self.is_connected = True
                    yield self._run_exclusively(self._connection.runOperation, "SET SESSION TIMEZONE TO 'UTC'")
                    yield self._run_exclusively(self._connection.runOperation, "SET application_name TO '%s-listener'" % self.profile_name)
                    self._connection.addNotifyObserver(self._notify)
                    logger.info('PostgresListener connected "{0}"'.format(self.profile_name))
                    self.on_connection_established(self)

                    while self.running:
                        try:
                            if self.is_waiting_for_txid_min:
                                yield util.wait(self.configuration['txid_poll_interval'])
                                yield self._check_txid_threshold()
                            else:
                                yield self.currently_pinging(util.wait(self.configuration['ping_interval']))
                                yield self.currently_pinging(self._ping())
                        except defer.CancelledError:
                            pass
                    
                except psycopg2.Error:
                    logger.exception('Database failure. Traceback follows')
                    failure_ = failure.Failure()
                    if self.is_connected:
                        self.on_connection_lost(failure_)
                    self.is_connected = False
                    self.on_connection_failed(failure_)
                    
                    yield self.currently(util.wait(self.configuration['retry_interval']))
                    
                except defer.CancelledError:
                    pass

        except Exception as e:
            logger.exception('Unhandled exception in PostgresListener.run')
        finally:
            if self._connection:
                self._connection.close()

    @defer.inlineCallbacks
    def listen(self, events):
        """Listens to every named event in *events, and returns a
        `DeferredQueue` that is populated with notifications.
        """
        dq = defer.DeferredQueue()
        for event in events:
            if not self._listeners[event]:
                self._listeners[event].add(dq)

                yield self._run_exclusively(self._connection.runOperation, 'LISTEN "%s"' % event)
                logger.info('"%s"-listener is now listening to "%s"' % (self.profile_name, event))
            
            self._listeners[event].add(dq)

        defer.returnValue(dq)

    @defer.inlineCallbacks
    def unlisten(self, queue):
        """ Stops listening for any event that populates *queue*.

        If this results in nothing listening to an event, we'll no longer listen to notifications of that event.
        """
        for listener_name, queues in self._listeners.items():
            if queue in queues:
                queues.discard(queue)
                if not queues:
                    yield self._run_exclusively(self._connection.runOperation, 'UNLISTEN "%s"' % listener_name)
                    logger.info('"%s"-listener is now NOT listening to "%s"' % (self.profile_name, listener_name))

    def wait_for_txid_min(self, txid):
        """ Returns a Deferred that is callbacked with the current
        txid_min when txid_min is >= txid. """
        d = defer.Deferred()
        heapq.heappush(self._txid_queue, (txid, d))

        if self._currently_pinging:
            # We don't want to wait a long time when we're waiting for
            # a txid-min, so cancel any ping-related waiting.
            self._currently_pinging.cancel()
        return d

    @property
    def is_waiting_for_txid_min(self):
        return bool(self._txid_queue)

    def _ping(self):
        return self._run_exclusively(self._connection.runOperation, "SELECT 'ping'")

    @defer.inlineCallbacks
    def _get_current_txid_min(self):
        rs = yield self._run_exclusively(self._connection.runQuery, 'SELECT txid_snapshot_xmin(txid_current_snapshot())')
        defer.returnValue(rs[0][0])

    @defer.inlineCallbacks
    def _check_txid_threshold(self):
        txid_min = yield self._get_current_txid_min()
        while self.running and self._txid_queue and self._txid_queue[0][0] <= txid_min:
            heapq.heappop(self._txid_queue)[1].callback(txid_min)

