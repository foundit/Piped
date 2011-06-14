# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.plugin import IPlugin
from twisted.trial import unittest

from piped import plugin, processing
from piped.test.plugins import autoloaded


class StubPluginManager(plugin.PluginManager):
    plugin_packages = [autoloaded]
    plugin_interface = IPlugin


class PluginManagerTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.configuration_manager = self.runtime_environment.configuration_manager
        self.manager = StubPluginManager()

    def get_available_plugin_names(self):
        return [name for name, cls in self.manager.get_all_plugins()]

    def test_default_loaded(self):
        self.manager.configure(self.runtime_environment)
        self.assertEquals(self.get_available_plugin_names(), ['foo'])

    def test_with_added_bundle(self):
        self.configuration_manager.set('plugins.bundles.included', ['piped.test.plugins.included'])

        self.manager.configure(self.runtime_environment)
        self.assertEquals(sorted(self.get_available_plugin_names()), ['bar', 'foo'])

    def test_with_added_bundle_and_disabled_package(self):
        self.configuration_manager.set('plugins.bundles.included', ['piped.test.plugins.included'])
        self.configuration_manager.set('plugins.disabled', ['piped.test.plugins.included'])

        self.manager.configure(self.runtime_environment)
        self.assertEquals(self.get_available_plugin_names(), ['foo'])

    def test_with_added_bundle_and_disabled_defaults(self):
        self.configuration_manager.set('plugins.bundles.included', ['piped.test.plugins.included'])
        self.configuration_manager.set('plugins.disabled', ['piped.test.plugins.autoloaded'])

        self.manager.configure(self.runtime_environment)
        self.assertEquals(self.get_available_plugin_names(), ['bar'])
