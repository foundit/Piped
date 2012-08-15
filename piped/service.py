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


class PipedService(object, service.Service):

    def __init__(self):
        self._currently = None
        self.currently = util.create_deferred_state_watcher(self)

    def configure(self, runtime_environment):
        pass

    def startService(self):
        if self.running:
            return

        service.Service.startService(self)
        if hasattr(self, 'run'):
            self.run()

    def stopService(self):
        service.Service.stopService(self)
        if self._currently:
            self._currently.cancel()
