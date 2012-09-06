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


class EngineManager(piped_service.PipedService):
    """Manages a SQLAlchemy Engine.

    There is no inherent support for threads. You are responsible for
    not blocking the reactor when using the engine, only using it from
    one thread, etc.

    Note that an engine by default uses a connection pool. The manager
    assumes that a connection will be available for checking out from
    the pool every now and then to ping. Pinging is done by executing
    a simple "SELECT 'ping'". If you exhaust the connection pool to
    the point where no connection can be used for pinging, the manager
    will assume error and restart.

    Configurables:

        engine: Dict that is passed directly to SQLAlchemy's
        `create_engine`-call. See SQLAlchemy's documentation on
        create_engine for full coverage, but these should get you
        started:

            url: For example 'postgresql://user:password@host:port/database'. Required.
            echo: Whether to log all SQL emitted. Defaults to false.

        ping_interval: How often to ping, in seconds. Defaults to every 10 seconds.
        retry_interval: How long to wait before reconnecting on error, in seconds. Defaults to 5 seconds.

        events: Dictionary mapping a SQLAlchemy engine-event to a
        `reflectAny`-name that will handle the event.

        checkout / checkin: List of SQL that is executed whenever a
        connection is checked out and when the connection is checked
        back in. For example, setting a timezone for the session when
        checking out a connection is common, and if used, releasing
        Postgres advisory locks should be done when checking a
        connection back in.


    Events:

        on_connection_established: A connection is established ---
        i.e. a ping has been successful. Invoked with the engine when established.

        on_connection_lost: An established connection was lost.

        on_connection_failed: The connection attempt failed.
    """

    proxy_factory = ConnectionProxy

    def __init__(self, profile_name, engine, events=None, checkout=None, checkin=None, ping_interval=10.0, retry_interval=5.0):
        super(EngineManager, self).__init__()
        self.profile_name = profile_name
        self.ping_interval = ping_interval
        self.retry_interval = retry_interval

        self.events = {} if events is None else events
        self.checkout = [] if checkout is None else checkout
        self.checkin = [] if checkin is None else checkin

        self.engine_configuration = copy.deepcopy(engine)
        self._fail_if_configuration_is_invalid()
        self.engine_configuration.setdefault('proxy', self.proxy_factory(self))

        self.is_connected = False

        self.engine = sa.engine_from_config(self.engine_configuration, prefix='')
        self._bind_events()

        self.on_connection_established = event.Event()
        self.on_connection_lost = event.Event()
        self.on_connection_failed = event.Event()

    def _bind_events(self):
        for event, list_of_handlers in self.events.items():
            for handler_name in list_of_handlers:
                sa.event.listen(self.engine, event, reflect.namedAny(handler_name))

        for sql_string in self.checkout:
            sa.event.listen(self.engine, 'checkout', functools.partial(self.on_checkout, sql_string))

        for sql_string in self.checkin:
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

    def _fail_if_configuration_is_invalid(self):
        if not self.engine_configuration:
            raise exceptions.ConfigurationError('no engine-configuration provided')
        if not self.engine_configuration.get('url'):
            raise exceptions.ConfigurationError('missing database-URL')
        if not self.ping_interval:
            detail = 'currently set to [{0!r}]'.format(configuration['ping_interval'])
            raise exceptions.ConfigurationError('Please specify a non-zero ping-interval', detail)

    @defer.inlineCallbacks
    def run(self):
        while self.running:
            try:
                # Connect and ping. The engine is a pool, so we're not
                # really establishing new connections all the time.
                logger.info('Attempting to connect to [{0}]'.format(self.profile_name))
                yield self.cancellable(threads.deferToThread(self._test_connectivity, self.engine))
                logger.info('Connected to [{0}]'.format(self.profile_name))

                if not self.is_connected:
                    self.is_connected = True
                    self.on_connection_established(self.engine)

                while self.running:
                    yield self.cancellable(util.wait(self.ping_interval))
                    yield self.cancellable(threads.deferToThread(self._test_connectivity, self.engine))

            except defer.CancelledError:
                if self.is_connected:
                    self.is_connected = False
                    self.on_connection_lost(failure.Failure())

                continue # Engine is disposed in finally.

            except Exception as e:
                logger.exception('Error with engine [{0}]'.format(self.profile_name))
                failure_ = failure.Failure()

                if self.is_connected:
                    logger.error('Lost connection to [{0}]'.format(self.profile_name))
                    self.is_connected = False
                    self.on_connection_lost(failure_)
                else:
                    self.on_connection_failed(failure_)

            finally:
                self.is_connected = False
                self.engine.dispose()

            yield self.cancellable(util.wait(self.retry_interval))

    def _test_connectivity(self, engine):
        if not self.running:
            return
        # This is actually checking in and out of an engine pool. We're not actually
        # establishing new connections all the time.
        connection = engine.connect()
        try:
            connection.execute("SELECT 'ping'")
        finally:
            connection.close()


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

    Although it takes the same configuration arguments as
    :class:`EventManager`, only `engine.url`, and `checkout` are used,
    in addition to the retry/ping/txid_poll-intervals. Since we are
    not dealing with a connection pool in this case, only the
    checkout-strings make sense in this context. Event handlers in
    `events` are not used.
    """

    def __init__(self, profile_name, url, checkout=None, checkin=None, events=None, ping_interval=10.0, retry_interval=5.0, txid_poll_interval=.1):
        super(PostgresListener, self).__init__()
        if not txpostgres and psycopg2:
            raise exceptions.ConfigurationError('cannot use PostgresListener without psycopg2 and txpostgres')

        self.profile_name = profile_name
        self.url = url
        self.ping_interval = ping_interval
        self.retry_interval = retry_interval
        self.txid_poll_interval = txid_poll_interval

        self.checkout = [] if checkout is None else checkout

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

    def _connect(self):
        url = urlparse.urlparse(self.url)
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
                    logger.info('PostgresListener attempting to connect to [{0}]'.format(self.profile_name))
                    yield self._run_exclusively(self._connect)
                    self.is_connected = True

                    yield self._run_exclusively(self._connection.runOperation, "SET application_name TO '%s-listener'" % self.profile_name)
                    for sql_string in self.checkout:
                        yield self._run_exclusively(self._connection.runOperation, sql_string)

                    self._connection.addNotifyObserver(self._notify)
                    logger.info('PostgresListener connected [{0}]'.format(self.profile_name))
                    self.on_connection_established(self)

                    while self.running:
                        try:
                            if self.is_waiting_for_txid_min:
                                yield self.cancellable(util.wait(self.txid_poll_interval))
                                yield self._check_txid_threshold()
                            else:
                                yield self.currently_pinging(self.cancellable(util.wait(self.ping_interval)))
                                yield self.currently_pinging(self._ping())
                        except defer.CancelledError:
                            pass

                    # We've cancelled, and is no longer running.
                    if self.is_connected:
                        self.is_connected = False
                        self.on_connection_lost(failure.Failure())

                except psycopg2.Error:
                    logger.exception('Database failure. Traceback follows')
                    failure_ = failure.Failure()
                    if self.is_connected:
                        self.is_connected = False
                        self.on_connection_lost(failure_)
                    else:
                        self.on_connection_failed(failure_)

                    yield self.cancellable(util.wait(self.retry_interval))

                except defer.CancelledError:
                    if self.is_connected:
                        self.is_connected = False
                        self.on_connection_lost(failure.Failure())

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
