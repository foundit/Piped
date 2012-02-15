#!/usr/bin/env python

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json
import os
import sys

import argparse
import twisted
from twisted.python import util
from twisted.scripts import twistd

import piped


def run_trial():
    from twisted.python import modules

    # We monkey patch PythonModule in order to avoid tests inside namespace packages
    # being run multiple times (see http://twistedmatrix.com/trac/ticket/5030)
    
    PythonModule = modules.PythonModule

    class PatchedPythonModule(PythonModule):
        def walkModules(self, *a, **kw):
            yielded = set()
            # calling super on PythonModule instead of PatchedPythonModule, since we're monkey patching PythonModule
            for module in super(PythonModule, self).walkModules(*a, **kw):
                if module.name in yielded:
                    continue
                yielded.add(module.name)
                yield module

    modules.PythonModule = PatchedPythonModule

    from twisted.scripts.trial import run
    run()


def run_piped():
    """ Wrapper around twistd to run piped. """
    parser = _make_parser()

    # we parse only the known arguments in case we're running a script
    args, remaining = parser.parse_known_args()

    if args.script:
        # if we are running a script, the configuration file is the script:
        args.conf = args.script
        # replace sys.argv with the arguments that the script might be interested in
        sys.argv = [sys.argv[0]] + remaining
    else:
        # otherwise, we have to re-parse the arguments to disallow unknown
        # arguments.
        args = parser.parse_args()

    # pass the configuration file as an environment variable that is read in the service.tac
    if args.conf:
        os.environ['PIPED_CONFIGURATION_FILE'] = args.conf

    if args.logfile == '-' and not args.nodaemon:
        parser.error('Daemons cannot log to stdout. Use -n to avoid daemonizing.')

    # append the command line overrides to the environment overrides
    overrides = json.loads(os.environ.get('PIPED_CONFIGURATION_OVERRIDES', '[]'))
    os.environ['PIPED_CONFIGURATION_OVERRIDES'] = json.dumps(overrides+args.override)

    twistd_config = _create_configuration_for_twistd(args)
    _run_twistd_with_config(twistd_config)


class VersionAction(argparse.Action):
    """ We provide our own argparse.Action in order to provide our own formatting. """

    def _find_plugin_versions(self):
        from twisted.python import modules

        versions = dict()

        plugin_module = modules.getModule('piped.plugins')
        plugin_module.load()

        for module in plugin_module.iterModules():
            versions[module.name] = getattr(module.load(), 'version', 'unknown version')

        return versions

    def __call__(self, parser, namespace, values, option_string=None):
        message = list()
        message.append('piped (the Piped process daemon) %s, %s' % (piped.version, twisted.version))
        message.append('')
        message.append('Contrib packages:')
        for name, version in sorted(self._find_plugin_versions().items()):
            message.append('\t%s: %s' % (name, version))
        message.append('')
        message.append('Python version: %s' % sys.version.replace('\n', ''))
        message.append('')

        parser.exit(message=os.linesep.join(message))


def _make_parser():
    """ Creates an ArgumentParser to parse the command line options. """
    parser = argparse.ArgumentParser(description='Piped process daemon.')

    parser.add_argument('-v', '--version', action=VersionAction, help='Print version information and exit.', nargs=0)

    parser.add_argument('-n', '--nodaemon', action='store_true', help='don\'t daemonize, don\'t use default umask of 0077')
    parser.add_argument('-l', '--logfile',
                        help='log to a specified file, - for stdout. %%d in the filename will be replaced with "[config_basename].log" (default: "-")',
                        default='-')
    parser.add_argument('-p', '--pidfile',
                        help='Name of the pidfile. %%d in the filename will be replaced by the default. (default: "[config_basename].pid")')
    parser.add_argument('-d', '--rundir', help='Change to a supplied directory before running (default: ".")', default='.')
    parser.add_argument('-D', action='store_true', help='Prevents cleaning of failures and gives more detailed tracebacks. Useful for debugging.')
    parser.add_argument('-O', '--override', action='append', metavar='path:value', default=list(),
                        help=('Configuration overrides that will be set after the configuration has been loaded. '
                              'This option may be used multiple times in order to set multiple overrides. '
                              'Example: --override=web.admin.port:8080'))

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--conf', help='Configuration file to use. [PIPED_CONFIGURATION_FILE]', metavar='config_file.yaml')
    group.add_argument('-s', '--script',
                       help=('Script file to use. If this argument is used, any unknown arguments passed to piped '
                             'will remain on sys.argv and can be consumed by the script.'), metavar='config_file.yaml')

    parser.add_argument('-r', '--reactor', help='Which reactor to use, (see --help-reactors for a list of possibilities)')
    parser.add_argument('--help-reactors', action='store_true', help='Display a list of possibly available reactor names.')

    return parser


def _create_configuration_for_twistd(args):
    """ Creates a suitable configuration for twisted based on the parsed arguments. """
    # twistd uses a dict-based configuration format instead of the argparse namespace-based one:
    config = dict(args.__dict__)

    config['python'] = util.sibpath(__file__, 'service.tac')
    config['no_save'] = True

    conf_without_extension = os.path.basename(config['conf']).rsplit('.', 1)[0]
    default_pidfile = '%s.pid'%conf_without_extension
    default_logfile = '%s.log'%conf_without_extension

    if not config.get('pidfile'):
        config['pidfile'] = default_pidfile

    config['pidfile'] = config['pidfile'].replace('%d', default_pidfile)
    config['logfile'] = config['logfile'].replace('%d', default_logfile)

    twistd_config = twistd.ServerOptions()
    twistd_config.update(config)

    if config['help_reactors']:
        twistd_config.opt_help_reactors()

    if config['reactor']:
        twistd_config.opt_reactor(config['reactor'])

    return twistd_config


def _run_twistd_with_config(twistd_config):
    if twistd_config['D']:
        # replace the default failure object with one that performs no cleaning
        from twisted.python import failure
        from piped import util
        failure._OriginalFailure = failure.Failure
        failure.Failure = util.NonCleaningFailure

        # and enable debugging on deferreds, so we can properly inspect post-mortem
        from twisted.internet import defer
        defer.setDebugging(True)

    sys.argv[0] = 'twistd' # pretend to be twistd, so that piped.log doesn't decide to bootstrap the logging system
    twistd.runApp(twistd_config)
