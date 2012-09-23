import functools

from twisted.internet import defer
from twisted.plugin import IPlugin
from twisted.application import service

from piped import plugin, plugins, util


class IPipedService(IPlugin, service.IService):
    """ Plugin-interface for services loaded by the `ServicePluginManager`.

    These services are instantiated when Piped starts, configured and
    then startService is invoked.

    API: Unstable.

    """

    def configure(self, runtime_environment):
        pass

    def is_enabled(self):
        """
        :return: True if this service should be enabled, False otherwise
        """
        pass


class ServicePluginManager(plugin.PluginManager, service.MultiService):
    """ Finds `IPipedService`-plugins, instantiates them, configures
    and sets itself as service-parent, resulting in the service being
    started. """
    plugin_packages = [plugins]
    plugin_interface = IPipedService

    def __init__(self):
        plugin.PluginManager.__init__(self)
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        plugin.PluginManager.configure(self, runtime_environment)
        self.setServiceParent(runtime_environment.application)

        for Service in self._plugins:
            service_instance = Service()
            service_instance.plugin_manager = self
            service_instance.configure(runtime_environment)

            if not service_instance.is_enabled():
                continue

            service_instance.setServiceParent(self)


class PipedService(util.Cancellable, service.MultiService):
    """ Base-class for `IPipedService`s. """

    def __init__(self):
        util.Cancellable.__init__(self)
        service.MultiService.__init__(self)

        self.runtime_environment = None

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

    def is_enabled(self):
        return True

    def startService(self):
        if self.running:
            return

        service.MultiService.startService(self)
        self.run()

    def run(self):
        """ Invoked by startService when it's time to do something useful.

        `startService()` takes care of checking that it's not already running, etc.

        Should stop when `self.running` is False and make sure that
        interaction with external services can be stopped, e.g. with
        the `cancellable()`-helper.
        """
        pass

    def stopService(self):
        """ Cancel any cancellables and stop the service. """
        if not self.running:
            return

        service.MultiService.stopService(self)
        self.cancel()


class PipedDependencyService(PipedService):
    """ A :class:`PipedService` that only runs when all its dependencies are provided. """

    def __init__(self):
        super(PipedDependencyService, self).__init__()
        self._running_with_dependencies = False

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

        self.self_dependency = runtime_environment.dependency_manager.add_dependency(self)
        self.self_dependency.on_ready += lambda dependency: self._consider_running_with_dependencies()
        self.self_dependency.on_lost += lambda dependency, reason: self.cancel()

    def run(self):
        """ Users of this class should usually use :func:`run_with_dependencies` instead. """
        return self._consider_running_with_dependencies()

    def _consider_running_with_dependencies(self):
        if self.running and self.self_dependency.is_ready and not self._running_with_dependencies:
            running_with_dependencies = self.cancellable(defer.maybeDeferred(self.run_with_dependencies))
            running_with_dependencies.addErrback(lambda failure: failure.trap(defer.CancelledError))

            self._running_with_dependencies = True
            running_with_dependencies.addBoth(self._stopped_running_with_dependencies)

            return running_with_dependencies

    def _stopped_running_with_dependencies(self, _):
        self._running_with_dependencies = False
        return _

    def run_with_dependencies(self):
        """ Invoked by _consider_running_with_dependencies when it's time to do something useful.

        When this function is called, the service should be running according to twisted, and all its
        dependencies are provided by Piped.

        Should stop when `self.running` is False and make sure that
        interaction with external services can be stopped, e.g. with
        the `cancellable()`-helper.
        """