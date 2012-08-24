from twisted.internet import defer
from twisted.plugin import IPlugin
from twisted.application import service
from zope import interface

from piped import plugin, exceptions, providers as piped_providers, plugins, util


class IPipedService(IPlugin, service.IService):

    def configure(self, runtime_environment):
        pass


class ServicePluginManager(plugin.PluginManager, service.MultiService):
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

    def __init__(self):
        service.MultiService.__init__(self)
        self._might_be_cancelled = None

    def configure(self, runtime_environment):
        pass

    def startService(self):
        if self.running:
            return

        self._might_be_cancelled = defer.Deferred()
        service.Service.startService(self)
        self.run()

    def run(self):
        pass

    def stopService(self):
        if not self.running:
            return

        self.cancel()
        service.MultiService.stopService(self)

    def cancel(self):
        # Swap it out before actually cancelling, so we don't
        # accidentally end up in a loop
        cancellable, self._might_be_cancelled = self._might_be_cancelled, defer.Deferred()

        if cancellable:
            cancellable.cancel()
            # In case nobody's actually using it, shut up.
            cancellable.addErrback(lambda failure: None)

    def cancellable(self, d):
        def _cancel(failure):
            d.cancel()
            return failure

        self._might_be_cancelled.addBoth(_cancel)
        return util.wait_for_first([d, self._might_be_cancelled])
