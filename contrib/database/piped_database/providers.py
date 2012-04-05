import sqlalchemy as sa
from piped_database import db as database
from twisted.application import service
from twisted.internet import reactor
from twisted.python import failure
from zope import interface

from piped import exceptions, resource


class DatabaseEngineProvider(object, service.MultiService):
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

        self._ensure_profile_has_manager(profile_name)
        self._bind_events_for_consumer(resource_dependency, profile_name)

    def _ensure_profile_has_manager(self, profile_name):
        if profile_name not in self._manager_for_profile:
            manager = database.EngineManager(self.database_profiles[profile_name], profile_name)
            manager.setServiceParent(self)
            self._manager_for_profile[profile_name] = manager

    def _bind_events_for_consumer(self, resource_dependency, profile_name):
        engine_manager = self._manager_for_profile[profile_name]

        engine_manager.on_connection_established += resource_dependency.on_resource_ready
        engine_manager.on_connection_failed += resource_dependency.on_resource_lost
        engine_manager.on_connection_lost += resource_dependency.on_resource_lost
