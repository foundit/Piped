# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import logging
import os
import sys
import json

import yaml
from twisted.internet import reactor

from piped import exceptions, resource, processing, service


logger = logging.getLogger('piped.service')


runtime_environment = processing.RuntimeEnvironment()
runtime_environment.configure()

application = runtime_environment.application


def _on_configuration_loaded():
    service_name = runtime_environment.configuration_manager.get('service_name', 'piped')

    # Set the process title, if we can.
    try:
        import setproctitle
        setproctitle.setproctitle(service_name)
    except ImportError:
        # It's just a nicety, though, so don't blow up if we can't.
        pass

    logger.info('Starting service "%s" (PID %i).'%(service_name, os.getpid()))

    provider_plugin_manager = resource.ProviderPluginManager()
    provider_plugin_manager.configure(runtime_environment)

    service_plugin_manager = service.ServicePluginManager()
    service_plugin_manager.configure(runtime_environment)
    
    # Move these into acting upon state changes.
    runtime_environment.dependency_manager.resolve_initial_states()


def bootstrap():
    configuration_file_path = os.environ.get('PIPED_CONFIGURATION_FILE', None)
    overrides = json.loads(os.environ.get('PIPED_CONFIGURATION_OVERRIDES', '[]'))
    try:
        _fail_if_no_configuration_file_is_specified(configuration_file_path)

        runtime_environment.configuration_manager.load_from_file(configuration_file_path)
        _handle_configuration_overrides(runtime_environment.configuration_manager, overrides)

        _on_configuration_loaded()
    except:
        logger.critical('Error while bootstrapping service.', exc_info=True)
        reactor.stop()


def _handle_configuration_overrides(configuration_manager, overrides):
    for override in overrides:
        # in yaml, a mapping uses a colon followed by a space, but we want to be able to
        # specify -O some.nested.option:42 on the command line, without the space, so we
        # add a space after the first colon in the override specification, as doing so does
        # not affect an otherwise correct yaml mapping.
        adjusted_override = override.replace(':', ': ', 1)
        override_as_dict = yaml.load(adjusted_override)

        if not isinstance(override_as_dict, dict):
            e_msg = 'Invalid override specification.'
            detail = 'Expected a yaml mapping, but got %r.' % override
            raise exceptions.ConfigurationError(e_msg, detail)

        for path, value in override_as_dict.items():
            logger.debug('Setting configuration override %r.'%path)
            configuration_manager.set(path, value)

def _fail_if_no_configuration_file_is_specified(configuration_file_path):
    if not configuration_file_path:
        e_msg = 'No configuration file specified.'
        detail = ('Either use the -c/--conf option of %r, or set the PIPED_CONFIGURATION_FILE environment '
                  'variable to the path of the configuration file.' % sys.argv[0])
        raise exceptions.ConfigurationError(e_msg, detail)


# The callLater is necessary so providers don't start before forking if we're daemonizing.
reactor.callLater(0, bootstrap)
