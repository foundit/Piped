import sqlalchemy as sa
from piped_database import db as database
from twisted.application import service
from twisted.internet import reactor
from twisted.python import failure
from zope import interface

from piped import exceptions, resource


class DatabaseMetadataProvider(object, service.MultiService):
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self._manager_for_profile = dict()
        self._keep_providing = False

    @property
    def database_profiles(self):
        return self.runtime_environment.get_configuration_value('database.profiles', dict())

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.setServiceParent(runtime_environment.application)
        resource_manager = runtime_environment.resource_manager
        for profile_name, profile_configuration in self.database_profiles.items():
            self._sanity_check_profile(profile_configuration)
            resource_manager.register('database.%s'%profile_name, provider=self)

    def add_consumer(self, resource_dependency):
        self._fail_if_configuration_is_invalid(resource_dependency.configuration)

        profile_name = resource_dependency.provider.split('.')[1]

        self._ensure_profile_has_metadata_manager(profile_name)
        self._bind_events_for_consumer(resource_dependency, profile_name)

        metadata_manager = self._manager_for_profile[profile_name]
        reactor.callInThread(self._connect_provider_and_suppress_exception, metadata_manager)

    def _sanity_check_profile(self, profile_configuration):
        pass

    def _ensure_profile_has_metadata_manager(self, profile_name):
        if profile_name not in self._manager_for_profile:
            manager = database.DatabaseMetadataManager(self.database_profiles[profile_name])
            manager.setServiceParent(self)
            self._manager_for_profile[profile_name] = manager

    def _bind_events_for_consumer(self, resource_dependency, profile_name):
        metadata_manager = self._manager_for_profile[profile_name]

        metadata_manager.on_connection_established += lambda _: resource_dependency.on_resource_ready(metadata_manager)
        metadata_manager.on_connection_failed += lambda reason: resource_dependency.on_resource_lost(reason)
        # TODO: Use a more specific exception class
        on_connection_lost = lambda: resource_dependency.on_resource_lost(failure.Failure(('Connection lost.',), exceptions.PipedError))
        metadata_manager.on_connection_lost += on_connection_lost

    def _fail_if_configuration_is_invalid(self, resource_configuration):
        pass

    @classmethod
    def _connect_provider_and_suppress_exception(cls, metadata_provider):
        try:
            metadata_provider.connect()
        except sa.exc.SQLAlchemyError:
            # This is logged and handled elsewhere.
            pass
