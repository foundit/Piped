import copy

import sqlalchemy as sa
from piped_database import db as database
from twisted.application import service
from twisted.internet import reactor
from twisted.python import failure
from zope import interface

from piped import exceptions, resource


try:
    import txpostgres
except ImportError:
    txpostgres = None


class DatabaseEngineProvider(object, service.MultiService):
    """Provides SQLAlchemy engines.

    See :class:`EngineManager` for per-engine configuration options.

    Engines are provided to "database.engine.engine_name". The
    engine's on_connection_established-, on_connection_failed- and
    on_connection_lost-events are tied to the on_resource_ready and
    on_resource_lost-events of the resulting :class:`ResourceDependency`.

    The same engine is provided to all consumers of it. See notes on
    threads and connection pools in the documentation of
    :class:`EngineManager`.

    Configuration example:

    .. code-block:: yaml

        database:
            engines:
                engine_name:
                    engine:
                        url: 'postgresql://user:password@host:port/database'
                        # echo: true
                        # see sqlalchemy.create_engine for more options.
                    checkout:
                        - "SET SESSION TIMEZONE TO 'UTC'"

    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._manager_for_profile = dict()

    @property
    def database_profiles(self):
        return self.runtime_environment.get_configuration_value('database.engines', dict())

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.setServiceParent(runtime_environment.application)
        resource_manager = runtime_environment.resource_manager

        for profile_name, profile_configuration in self.database_profiles.items():
            resource_manager.register('database.engine.%s'%profile_name, provider=self)

    def add_consumer(self, resource_dependency):
        profile_name = resource_dependency.provider.split('.')[2]

        manager = self._ensure_profile_has_manager(profile_name)
        self._bind_events_for_consumer(resource_dependency, profile_name)
        if manager.is_connected:
            resource_dependency.on_resource_ready(manager.engine)

    def _ensure_profile_has_manager(self, profile_name):
        if profile_name not in self._manager_for_profile:
            manager = database.EngineManager(profile_name, **self.database_profiles[profile_name])
            manager.setServiceParent(self)
            self._manager_for_profile[profile_name] = manager
        return self._manager_for_profile[profile_name]

    def _bind_events_for_consumer(self, resource_dependency, profile_name):
        engine_manager = self._manager_for_profile[profile_name]

        engine_manager.on_connection_established += resource_dependency.on_resource_ready
        engine_manager.on_connection_failed += resource_dependency.on_resource_lost
        engine_manager.on_connection_lost += resource_dependency.on_resource_lost


class PostgresListenProvider(object, service.MultiService):
    """Provides :class:`PostgresListener`s.

    Obviously, this only works with Postgres backends.

    The configuration used to configure the
    :class:`DatabaseEngineProvider` is also the basis for this
    provider.

    The listeners are available at "database.listener.engine_name".

    Note that just the *url*-parameter is used from *engine*, and
    event-wise, only *checkout* is used --- i.e. SQL that should be
    issued when the connection is established, such as setting a
    timezone. The *checkin*- and *events*-handlers used by
    :class:`EngineManager` is *not* used.

    Status: Alpha.
    API-stability: Unstable.

    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._listener_for_profile = dict()

    @property
    def database_profiles(self):
        return self.runtime_environment.get_configuration_value('database.engines', dict())

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.setServiceParent(runtime_environment.application)
        resource_manager = runtime_environment.resource_manager

        for profile_name, profile_configuration in self.database_profiles.items():
            resource_manager.register('database.listener.%s'%profile_name, provider=self)

    def add_consumer(self, resource_dependency):
        if not txpostgres:
            raise exceptions.ConfigurationError('txpostgres is not installed')

        profile_name = resource_dependency.provider.split('.')[2]

        listener = self._ensure_profile_has_listener(profile_name)
        self._bind_events_for_consumer(resource_dependency, profile_name)
        if listener.is_connected:
            resource_dependency.on_resource_ready(listener)

    def _ensure_profile_has_listener(self, profile_name):
        if profile_name not in self._listener_for_profile:
            configuration = copy.deepcopy(self.database_profiles[profile_name])
            configuration['url'] = configuration.pop('engine')['url']

            listener = database.PostgresListener(profile_name, **configuration)
            listener.setServiceParent(self)
            self._listener_for_profile[profile_name] = listener
        return self._listener_for_profile[profile_name]

    def _bind_events_for_consumer(self, resource_dependency, profile_name):
        engine_listener = self._listener_for_profile[profile_name]

        engine_listener.on_connection_established += resource_dependency.on_resource_ready
        engine_listener.on_connection_failed += resource_dependency.on_resource_lost
        engine_listener.on_connection_lost += resource_dependency.on_resource_lost
