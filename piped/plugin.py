# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import functools
import pkgutil
import re
import warnings

from twisted import plugin as twisted_plugin
from twisted.python import reflect, failure, log as twisted_log

from piped import event, exceptions, log, util


def _plugin_error_handler(package):
    f = failure.Failure()
    f.trap(ImportError)

    logger = log.error

    # set the logger to info when running unit tests in order to avoid flooding
    if util.in_unittest():
        logger = log.info

    logger('Failed loading plugins from package %r: %s(%s)' % (package.__name__, f.type.__name__, f.getErrorMessage()))


def getPlugins(interface, package):
    original_err = twisted_log.err
    try:
        twisted_log.err = functools.partial(_plugin_error_handler, package)
        for plugin in twisted_plugin.getPlugins(interface, package):
            yield plugin
    finally:
        twisted_log.err = original_err


class PluginManager(object):
    # TODO: Docs
    plugin_interface = None
    plugin_packages = None
    plugin_configuration_path = 'plugins'

    def __init__(self):
        self._plugins = set()
        self._plugin_factory_by_name = dict()
        self._providers_by_keyword = dict()
        self.runtime_environment = None
        self.plugins_loaded = False

        self.on_plugins_loaded = event.Event()
        self.on_package_paths_updated = event.Event()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.plugins_config = runtime_environment.get_configuration_value(self.plugin_configuration_path, dict())
        self.load_plugins()

    def load_plugins(self, reload=False):
        """ Return the plugins implementing `plugin_interface` found in
        the mentioned `plugin_packages`. """

        if not self.plugins_loaded or reload:
            self._plugins = set()
            for package in self.plugin_packages:
                for plugin in self._get_plugins_from_package(package):
                    self._plugins.add(plugin)

            for package in self._get_bundled_packages():
                for plugin in self._get_plugins_from_package(package):
                    self._plugins.add(plugin)

            # Map the plugin's name to the class.
            self._plugin_factory_by_name = dict()
            for plugin in self._plugins:
                self._register_plugin(plugin)

            self.plugins_loaded = True

        self.on_plugins_loaded(self._plugins)

    def _get_bundled_packages(self):
        """ Return a list of additional packages that are configured as bundles in the
        configuration file.
        """
        packages = list()

        for bundle_name, bundle_config in self.plugins_config.get('bundles', dict()).items():
            self._fail_if_bundle_configuration_is_invalid(bundle_config)

            for package_name in bundle_config:
                try:
                    package = reflect.namedAny(package_name)
                except reflect.InvalidName, e:
                    e_msg = e.args[0]
                    detail = '%r does not seem to be a valid package.' % package_name
                    hint = 'Ensure that sys.path is set correctly.'
                    raise exceptions.ConfigurationError(e_msg, detail, hint)

                packages.append(package)

        return packages

    def _is_disabled(self, package_name):
        for disabled_pattern in self.plugins_config.get('disabled', list()):
            if re.match(disabled_pattern, package_name):
                return True
        return False

    def _get_plugins_from_package(self, package):
        self._fail_if_not_a_package(package)

        if self._is_disabled(package.__name__):
            return

        sub_packages_names = [name for (loader, name, ispkg) in
                              pkgutil.walk_packages(package.__path__, package.__name__ + '.')
                              if ispkg and not self._is_disabled(name)]

        for sub_package_name in sub_packages_names:
            for plugin in getPlugins(self.plugin_interface, reflect.namedAny(sub_package_name)):
                yield plugin

        for plugin in twisted_plugin.getPlugins(self.plugin_interface, package):
            yield plugin

    def _fail_if_not_a_package(self, package):
        if not hasattr(package, '__path__'):
            e_msg = 'Expected a package, not a module.'
            detail = 'Loading plugins from modules are not supported.'

            module_name = package.__name__
            suggested_package_name = module_name.rsplit('.', 1)[0]

            hint = 'Attempted to load plugins from %r, maybe you meant %r?' % (module_name, suggested_package_name)

            raise exceptions.ConfigurationError(e_msg, detail, hint)

    def _fail_if_bundle_configuration_is_invalid(self, bundle_config):
        if not isinstance(bundle_config, list):
            e_msg = 'Invalid plugin bundle configuration'
            detail = 'A plugin bundle configuration should be a list. Found: %r' % bundle_config
            raise exceptions.ConfigurationError(e_msg, detail)

    def _register_plugin(self, plugin):
        name = getattr(plugin, 'name', None) or reflect.fullyQualifiedName(plugin)
        self._fail_if_plugin_name_is_already_registered(plugin, name)

        self._plugin_factory_by_name[name] = plugin
        provided_keywords = getattr(plugin, 'provides', [])
        for keyword in provided_keywords:
            self._providers_by_keyword.setdefault(keyword, []).append(name)

    def _fail_if_plugin_name_is_already_registered(self, plugin, plugin_name):
        if plugin_name in self._plugin_factory_by_name:
            e_msg = 'multiple plugins with the same name: %s ' % plugin_name
            details = (plugin, self._plugin_factory_by_name[plugin_name])
            detail = 'Attempted to register "%s" with that name, but it is already registered by "%s" ' % details
            raise exceptions.ConfigurationError(e_msg, detail)

    def get_plugin_factory(self, plugin_name):
        """ Return plugin factory for *plugin_name*. """
        plugin_factory = self._plugin_factory_by_name.get(plugin_name)
        if not plugin_factory:
            self._fail_because_of_nonexisting_plugin(plugin_name)
        return plugin_factory

    def _fail_because_of_nonexisting_plugin(self, plugin_name):
        if not self.plugins_loaded:
            warnings.warn('Tried getting a plugin without first loading the plugin manager.')
        e_msg = 'invalid plugin name: "%s" ' % (plugin_name, )
        details = dict(
            available_plugins=sorted(self._plugin_factory_by_name.keys()),
            interface=self.plugin_interface.__name__,
            packages=sorted([package.__name__ for package in self.plugin_packages + self._get_bundled_packages()]),
        )
        detail = ('No plugin with the provided name was found.\n\nAvailable plugins: %(available_plugins)s\n\n'
                  'Looking for plugins implementing this interface: %(interface)s\n\n'
                  '... in these packages: %(packages)s\n' % details)
        hint = ('If you have not simply misspelled, ensure that the right interfaces are implemented '
                'and that the right packages are loaded. ')
        raise exceptions.ConfigurationError(e_msg, detail, hint)

    def get_providers_of_keyword(self, keyword):
        """ Return list of plugin names that provide *keyword*. """
        return self._providers_by_keyword.get(keyword, [])

    def get_all_plugins(self):
        """ Returns a tuple of (plugin_name, plugin_factory)-tuples. """
        return tuple(self._plugin_factory_by_name.items())
