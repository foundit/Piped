# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import os
import warnings
from pprint import pformat

import yaml
from twisted.python import filepath

# We don't use yamlutil directly here, but import it as it registers custom constructors and representers.
from piped import yamlutil
from piped import log, exceptions, util


def _warn_because_file_is_loaded_multiple_times(path, visited_files):
    msg = 'configuration file loaded multiple times: "%s"' % path

    detail = ('During the handling of configuration file includes, '
              'a previously included file was encountered.')

    existing_files = [file for file in visited_files if os.path.exists(file)]
    hint = ('Ensure that the "includes" do not accidentally recurse. '
            'Visited files in the following order:\n ' + '\n\n       * '.join(existing_files))

    # issue a simple warning, log more details with hints:
    warnings.warn(exceptions.ConfigurationWarning(msg))
    log.debug('WARNING: %s' % msg)
    log.debug('DETAIL: %s' % detail)
    log.debug('HINT: %s' % hint)


class ConfigurationManager(object):
    """ Responsible for loading and accessing the configuration.

    Example configuration::

        includes:
            - !filepath ~/common.yml

        some_module:
            bar: baz
            foo: 123

    The included file may itself define includes, and the loading will be performed
    recursively. The included configuration will be merged with the current
    configuration, and values that aren't mappings from the current configuration will
    override any value from the included configuration.

    For example, in common.yml::

        some_module.
            zip: zap
            bar:
                 more: options

    With the earlier configuration as a base, would result in the following configuration
    after loading::

        some_module
            bar: baz
            foo: 123
            zip: zap

    """
    def __init__(self):
        self._config = dict()

    def configure(self, runtime_environment):
        """ Configures this manager with the runtime environment.

        :type runtime_environment: `.processing.RuntimeEnvironment`
        """
        self.runtime_environment = runtime_environment

    def load_from_file(self, filename):
        """ Load the configuration by parsing the data in the given file.

        For an overview of how configuration is loaded, see :doc:`/topic/configuration`
        """
        filename = util.expand_filepath(filename)
        log.debug('Loading configuration from: %s'%filename)
        self._fail_if_configuration_file_does_not_exist(filename)

        base_config = yaml.load(open(filename))
        complete_config = self._load_config(base_config, [filename])
        self._resolve_aliases(complete_config)

        self._config = complete_config

        log.debug('Loaded configuration: '+pformat(complete_config))

    def _load_config(self, config, visited_files):
        """ Handles the configuration object, including referenced configurations as
            specified. Also handles replacement of special options like runmodes.
        """
        current_configuration_file = visited_files[-1]

        # if a configuration file does not contain anything, yaml.load returns None,
        # but we consider an empty configuration file to be an empty dict instead
        if config is None:
            config = dict()

        self._fail_if_configuration_is_invalid(config, current_configuration_file)

        includes = config.get('includes', dict())

        config = self._load_includes(config, includes, visited_files)

        # see if we have any special options for this mode
        if 'runmodes' in config:
            # currently we only support "unittest" as the runmode:
            if util.in_unittest():
                config = util.merge_dicts(config, config['runmodes'].get('unittest', dict()), replace_primitives=True)

        return config

    def _load_includes(self, current_config, include_paths, visited_files):
        """ Recursively handles includes of other configuration-files. """
        flattened_include_paths = list()

        for i, include_path in enumerate(list(include_paths)):
            if isinstance(include_path, (basestring, filepath.FilePath)):
                flattened_include_paths.append(dict(prefix='', path=include_path))

            if isinstance(include_path, dict):
                for key, value in sorted(include_path.items()):
                    flattened_include_paths.append(dict(prefix=key, path=value))

        for include_item in flattened_include_paths:
            include_prefix = include_item['prefix']
            include_path = util.expand_filepath(include_item['path'])

            if include_path in visited_files:
                _warn_because_file_is_loaded_multiple_times(include_path, visited_files)

            visited_files.append(include_path)

            self._fail_if_configuration_file_does_not_exist(include_path)

            try:
                included_config = yaml.load(open(include_path))
                included_config = self._load_config(included_config, visited_files)

                if include_prefix:
                    prefixed_included_config = dict()
                    util.dict_set_path(prefixed_included_config, include_prefix, included_config)
                    included_config = prefixed_included_config

                current_config = util.merge_dicts(included_config, current_config, replace_primitives=True)
            except exceptions.ConfigurationError:
                raise
            except Exception, why:
                raise exceptions.ConfigurationError('Error in configuration file %s: %s'%(include_path, why))

        return current_config

    def _resolve_aliases(self, config):
        # find all :class:`yamlutil.Alias`\ss and update them
        for alias_node in list(self._find_instances(config, yamlutil.Alias)):
            data = util.dict_get_path(config, alias_node.path)
            alias_node.update(data)

    def _find_instances(self, dict_or_list, *instances):
        """ Find all instances of the specified class(es) in the dict_or_list-like. """
        iterator = dict_or_list
        if hasattr(dict_or_list, 'values'):
            iterator = dict_or_list.values()
        for possible_instance in iterator:
            if isinstance(possible_instance, instances):
                yield possible_instance
            if hasattr(possible_instance, '__iter__'):
                for instance in self._find_instances(possible_instance, *instances):
                    yield instance

    def _fail_if_configuration_is_invalid(self, config, current_configuration_file):
        if not isinstance(config, dict):
            e_msg = 'Invalid configuration.'
            detail = 'The configuration file %r did not contain a mapping.' % current_configuration_file
            raise exceptions.ConfigurationError(e_msg, detail)

    def _fail_if_configuration_file_does_not_exist(self, include_path):
        if not os.path.exists(include_path):
            e_msg = 'Could not find configuration file.'
            detail = 'During the loading of includes, the following file were not found: %r.' % include_path
            hint = ('Make sure the current working directory is correct, and that the included '
                    'configuration file exists.')
            raise exceptions.ConfigurationError(e_msg, detail, hint)

    def get(self, path_or_paths, fallback=None):
        """ Return the first value piped in the configuration for the
        key paths given in *path_or_paths*. If no paths result in a
        value, *fallback* is returned.

        .. seealso::

            :func:`~piped.util.dict_get_path`
                for more on the behaviour of getting with key paths.
        """
        return util.dict_get_path(self._config, path_or_paths, fallback)

    def set(self, path, value):
        """ Sets the configuration key specified by *path* to *value*.
        
        .. seealso::

            :func:`~piped.util.dict_set_path`
                for more on the behaviour of setting with key paths.
        """
        util.dict_set_path(self._config, path, value)
        return value

    def setdefault(self, path, value):
        """ Sets the configuration key specified by *path* to *value*,
        unless a value is already defined for that path. Returns the
        value of *path* --- i.e. either *value* or the already
        existing value.

        .. :seealso: ::

            :ref:`~piped.util.dict_setdefault_path`
                for more on the behaviour of setting with key paths.
        """
        return util.dict_setdefault_path(self._config, path, value)
