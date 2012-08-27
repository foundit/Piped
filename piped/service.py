from twisted.internet import defer
from twisted.plugin import IPlugin
from twisted.application import service
from zope import interface

from piped import plugin, exceptions, providers as piped_providers, plugins, util


class IPipedService(IPlugin, service.IService):
    """ Plugin-interface for services loaded by the `ServicePluginManager`.

    These services are instantiated when Piped starts, configured and
    then startService is invoked.

    API: Unstable.

    """

    def configure(self, runtime_environment):
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
            service_instance.setServiceParent(self)


class PipedService(object, service.MultiService):
    """ Base-class for `IPipedService`s. """

    def __init__(self):
        service.MultiService.__init__(self)
        self._might_be_cancelled = None
        self.runtime_environment = None

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

    def startService(self):
        if self.running:
            return

        self._might_be_cancelled = defer.Deferred()
        service.Service.startService(self)
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

        self.cancel()
        service.MultiService.stopService(self)

    def cancel(self):
        """ Cancels cancellables. """
        # Swap it out before actually cancelling, so we don't
        # accidentally end up in a loop
        cancellable, self._might_be_cancelled = self._might_be_cancelled, defer.Deferred()

        if cancellable:
            cancellable.cancel()
            # In case nobody's actually using it, shut up.
            cancellable.addErrback(lambda failure: failure.trap(defer.CancelledError))

    def cancellable(self, d):
        """Takes a deferred *d* and returns another deferred that will
        callback/errback with whatever *d* callbacks/errbacks with, or
        errback with a `CancelledError` if `cancel()` is invoked.

        If `cancel()` is invoked, then the deferred *d* is also
        cancelled.

        This is useful when you wait for deferreds that do not
        necessarily stop when this service stops --- especially when
        the process will continue on/restart this services and the
        "abandoned" deferreds and their callback-chains consume a lot
        of memory.
        """
        def _cancel(failure):
            d.cancel()
            return failure

        self._might_be_cancelled.addBoth(_cancel)
        return util.wait_for_first([d, self._might_be_cancelled])
