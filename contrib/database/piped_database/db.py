# -*- test-case-name: piped.test.test_database -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import warnings

import sqlalchemy as sa
import sqlalchemy.interfaces
from twisted.application import service
from twisted.internet import defer, reactor, threads
from twisted.python import failure

from piped import event, exceptions, util, log


class DatabaseError(exceptions.PipedError):
    pass


class DatabaseMetadataManager(object, service.Service):
    """ Class that handles all establishing, losing, and re-establishing
    connections to a database.

    TODO: Remove false limitation to MySQL and Postgres.
    """

    def __init__(self, database_configuration):
        self.database_configuration = database_configuration

        for key in ('database_name', 'user', 'password'):
            setattr(self, key, database_configuration.get(key))

        # Options for which we'll provide a default:
        for key, default in (('protocol', 'postgresql'), ('pool_size', 10), ('timeout', 1), ('reconnect_wait', 10),
                             ('host', '')): # The empty string as a host means use local socket
            setattr(self, key, database_configuration.get(key, default))
        if not self.protocol in ('postgresql', 'mysql'):
            raise ValueError('The provider currently only works with PostgreSQL and MySQL, not ' + self.protocol)

        # Options to provide to the underlying DBAPI-driver.
        self.dbapi_options = database_configuration.get('dbapi_options', {})

        self.is_connected = False
        self.engine = None
        self.port = None # The default port is set by _configure_(postgres|mysql)

        self.on_connection_established = event.Event() #: Event called when a connection has been established. Provides a metadata instance as argument.
        self.on_connection_lost = event.Event()
        self.on_connection_failed = event.Event()
        log_failure = lambda failure: log.error('A connection to "%s" failed: %s' % (self.database_name, failure.getErrorMessage()))
        self.on_connection_failed += log_failure
        self.on_connection_failed += lambda failure: self.keep_reconnecting_and_report()

        self._reconnect_deferred = None
        self._should_reflect = False
        self._has_reflected = False
        self.metadata = None
        self.metadata_factory = sa.MetaData

    def connect(self, reflect=True, existing_metadata=None):
        """ Connect to the database.

        When a connection has been established, the
        `on_connection_established` is invoked with the metadata.
        If connecting fails, the `on_connection_failed` is invoked
        with the failure.

        :param reflect: If true, reflects the tables and their relations.

        :param existing_metadata: If provided, use this existing
            metadata instead of creating a metadata instance.

        :param reconnect_on_failure: If true, keeps reconnecting if the initial
            attempt fails.
        """
        if self.is_connected:
            return

        self._should_reflect = reflect

        self._configure_driver()
        self._make_metadata(existing_metadata)

        try:
            self._try_connecting()
        except sa.exc.SQLAlchemyError, e:
            log.error('Could not connect to database "%s": %s' % (self.database_name, e))
            reactor.callFromThread(self.on_connection_failed, failure.Failure())
            raise
        else:
            self.is_connected = True
            log.info('Connected to database "%s"' % self.database_name)
            reactor.callFromThread(self.on_connection_established, self.metadata)

    def _configure_driver(self):
        if self.protocol == 'postgresql':
            self._configure_postgres()
        else:
            self._configure_mysql()

    def _configure_mysql(self):
        """ Configure a MySQL engine. """
        self.port = self.database_configuration.get('port', 3306)

        self.dbapi_options.setdefault('connect_timeout', self.timeout)

        # Unicode dammit! :)
        self.dbapi_options.setdefault('charset', 'utf8')
        self.dbapi_options.setdefault('use_unicode', '1')

        dsn = sa.engine.url.URL('mysql', self.user, self.password, self.host, self.port,
                                self.database_name, self.dbapi_options)


        self.engine = sa.create_engine(dsn, convert_unicode=True, proxy=ConnectionProxy(self))

    def _configure_postgres(self):
        """ Configure a PostgreSQL engine. """
        self.port = self.database_configuration.get('port', 5432)
        engine_description = 'postgresql://%s/%s' % (self.host, self.database_name)
        # The effort here is to get to Psycopg2's connect_timeout, which it for some
        # reason does *NOT* provide through dbapi-options.
        pool = sa.pool.QueuePool(self._raw_connect_postgres, pool_size=self.pool_size)
        self.engine = sa.create_engine(engine_description, pool=pool, proxy=ConnectionProxy(self))

    def _raw_connect_postgres(self):
        """ Gets a raw dbapi-connection to the database """
        import psycopg2
        c_string = "user='%s' host='%s' port='%i' dbname='%s' password='%s' connect_timeout=%i" % \
            (self.user, self.host, self.port, self.database_name, self.password, self.timeout)

        try:
            return psycopg2.connect(c_string)
        except psycopg2.OperationalError:
            raise sa.exc.OperationalError(c_string, {}, None)

    def _make_metadata(self, existing_metadata):
        if existing_metadata:
            self.metadata = existing_metadata
            self.metadata.bind  = self.engine
        else:
            self.metadata = self.metadata_factory(self.engine)

    def _try_connecting(self):
        if self._should_reflect:
            self._reflect()
        else:
            self.test_connectivity()

    def _reflect(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', sa.exc.SAWarning)
            self.metadata.reflect()
        self._has_reflected = True

    def disconnect(self):
        """ Disconnect from the database.

        This closes all active connections in the underlying
        connection pool, and calls the `on_connection_lost`. """
        self.metadata.bind.dispose()
        reactor.callFromThread(self.on_connection_lost)

    def test_connectivity(self):
        """ Test the connectivity and return `True` if the
        connectivity works --- throw an exception otherwise.

        :Exceptions:
            :exc:`sa.exc.SQLAlchemyError`
               An exception indicating why the connectivity is broken.

        :todo: What if the pool is full and the connect() blocks forever? """
        assert self.metadata, "No metadata, have never been connected in the first place."
        self.metadata.bind.connect().execute(sa.select([1])).scalar() == 1
        self.is_connected = True

    def reconnected(self):
        """ Called when a connection has been re-established.

        This will do reflection if necessary and call the
        `on_connection_established`. It will also callback any
        active reconnection-deferred.
        """
        if self._should_reflect and not self._has_reflected:
            self._reflect()

        reactor.callFromThread(self.on_connection_established, self.metadata)

    def keep_reconnecting_and_report(self):
        """ Returns a deferred that is callbacked when connectivity
        has been re-established. """
        self._warn_if_reactor_is_not_running()
        if not self._reconnect_deferred:
            self._reconnect_deferred = self._test_connection_until_working()
        return self._reconnect_deferred

    @defer.inlineCallbacks
    def _test_connection_until_working(self):
        """ Keep trying to connect. Calls itself every
        `wait_between_reconnect_tries` seconds. """
        while self.running:
            try:
                log.debug('Trying to connect to database "%s"' % self.database_name)
                yield threads.deferToThread(self.test_connectivity)
                yield threads.deferToThread(self.reconnected)
                break

            except sa.exc.SQLAlchemyError, e:
                reactor.callFromThread(self.on_connection_failed, failure.Failure())
                log.error('Could not connect to database "%s": %s' % (self.database_name, e))
            yield util.wait(self.reconnect_wait)

        self._reconnect_deferred = None
        defer.returnValue(self.metadata)

    @classmethod
    def _warn_if_reactor_is_not_running(cls):
        if reactor.running or util.in_unittest():
            return

        msg = 'connection attempted without running Twisted reactor'
        detail = ('The reconnect mechanism is based on having a running Twisted reactor. '
                  'Either ensure that Twisted is running, don\'t expect reconnecting to happen or '
                  'implement a checker thread.')
        warning = exceptions.PipedWarning(msg, detail)
        warnings.warn(warning)


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
