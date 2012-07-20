# -*- test-case-name: piped.test.test_database -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import collections
import logging
import urlparse
import warnings

import sqlalchemy as sa
import sqlalchemy.interfaces
from twisted.application import service
from twisted.internet import defer, reactor, threads
from twisted.python import failure, reflect

from piped import event, exceptions, util


logger = logging.getLogger(__name__)


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
        try:
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

                except sa.exc.OperationalError as e:
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

        except Exception as e:
            logger.exception('Unhandled exception in _connect_and_stay_connected. Traceback follows')
        finally:
            self.engine.dispose()

    def _test_connectivity(self, engine):
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


class PostgresListener(object, service.Service):

    def __init__(self, configuration, profile_name):
        if not txpostgres and psycopg2:
            raise exceptions.ConfigurationError('cannot use PostgresListener without psycopg2 and txpostgres')

        self.configuration = configuration
        self.profile_name = profile_name
        self.currently = util.create_deferred_state_watcher(self)
        self._connection = None
        self._listeners = collections.defaultdict(set)

        self.is_connected = False
        self.on_connection_established = event.Event()
        self.on_connection_lost = event.Event()
        self.on_connection_failed = event.Event()

        configuration.setdefault('ping_interval', 10.0)
        configuration.setdefault('retry_interval', 5.0)

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
        
    def startService(self):
        if self.running:
            return

        service.Service.startService(self)
        self._connect_and_listen()

    def stopService(self):
        if not self.running:
            return

        service.Service.stopService(self)
        if self._connection:
            self._connection.close()

    def _notify(self, notification):
        for queue in self._listeners[notification.channel]:
            queue.put(notification)

    @defer.inlineCallbacks
    def _connect_and_listen(self):
        try:
            while self.running:
                try:
                    logger.info('PostgresListener attempting to connect to "{0}"'.format(self.profile_name))
                    yield self.currently(self._connect())
                    self.is_connected = True
                    yield self.currently(self._connection.runOperation("SET SESSION TIMEZONE TO 'UTC'"))
                    yield self.currently(self._connection.runOperation("SET application_name TO '%s-listener'" % self.profile_name))                    
                    self._connection.addNotifyObserver(self._notify)
                    logger.info('PostgresListener connected "{0}"'.format(self.profile_name))
                    self.on_connection_established(self)

                    try:
                        while self.running:
                            yield self.currently(util.wait(self.configuration['ping_interval']))
                            yield self.currently(self._test_connectivity())
                    except defer.CancelledError:
                        pass
                    
                except psycopg2.Error:
                    failure_ = failure.Failure()
                    if self.is_connected:
                        self.on_connection_lost(failure_)
                    self.is_connected = False
                    self.on_connection_failed(failure_)
                    
                    yield self.currently(util.wait(self.configuration['retry_interval']))
                    
                except defer.CancelledError:
                    pass

        except Exception as e:
            logger.exception('Unhandled exception in _connect_and_listen. Traceback follows')

    @defer.inlineCallbacks
    def listen(self, events):
        dq = defer.DeferredQueue()
        for event in events:
            if not self._listeners[event]:
                yield self._connection.runOperation('LISTEN "%s"' % event)
                logger.info('"%s"-listener is now listening to "%s"' % (self.profile_name, event))
            
            self._listeners[event].add(dq)

        defer.returnValue(dq)

    @defer.inlineCallbacks
    def unlisten(self, queue):
        1/0
        for listener_name, queues in self._listeners.items():
            if queue in queues:
                queues.discard(queue)
                if not queues:
                    yield self._connection.runOperation('UNLISTEN "%s"' % listener_name)
                    logger.info('"%s"-listener is now NOT listening to "%s"' % (self.profile_name, listener_name))
                    

    def _test_connectivity(self):
        return self._connection.runOperation("SELECT 'ping'")
