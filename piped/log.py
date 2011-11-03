# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
""" The piped logging system.

Initialization of piped.log
---------------------------

After the configuration has been loaded, the first system that is configured
is the logging system. This is done by a call to :func:``piped.log.configure``,
which uses the configuration to look up and instantiate an implementation of the
`ILogger` interface. If no logger is specified, the `DefaultLogger` is used.

When the logger has been configured, the global variables ``debug``, ``info``,
``warn``, ``error`` and ``critical`` is set to loggers corresponding functions,
which makes them available to the rest of the system.

.. note::
    Plugin authors are encouraged to use ``from piped import log`` and use
    ``log.info`` etc. to perform their logging calls instead of importing the
    functions directly.

    This is in order to support the logger being changed after the initial
    system startup.


"""
import inspect

from twisted.internet import reactor
from twisted.python import log, reflect, failure
from zope import interface


# different logging levels
level_value_by_name = dict(
    DEBUG = 10,
    INFO = 20,
    WARN = 30,
    ERROR = 40,
    CRITICAL = 50,
)


class ILogger(interface.Interface):
    def configure(self, runtime_environment):
        """ Configure the logger. """

    def debug(self, msg):
        """ Log a debugging message. """

    def info(self, msg):
        """ Log an informational message. """

    def warn(self, msg):
        """ Log a warning message. """

    def error(self, msg):
        """ Log an error message. """

    def critical(self, msg):
        """ Log a critical message. """


class DefaultLogger(object):
    """ The default piped logger.

    When this logger is asked to log a message, it uses thresholds defined
    in the configuration to determine whether the message should be written
    to the log or ignored.

    Example configuration::

        logging:
            default: WARN
            modules:
                piped:
                    conf:
                        # allow informational messages from piped.conf
                        __threshold__: INFO
                    processors:
                        # but only error messages from piped.processors
                        __threshold__: ERROR
                        util_processors:
                            # .. unless it is from piped.processors.util_processors,
                            # that we want to show debugging messages from
                            __threshold__: DEBUG

    The deepest matching module threshold is used.
    """

    interface.implements(ILogger)
    log_config = None

    def configure(self, runtime_environment):
        self.log_config = runtime_environment.get_configuration_value('logging')

    def _should_log(self, level, by_module=None):
        if not self.log_config:
            # if we don't have a configuration, just log for WARN and above.
            return True if level_value_by_name[level] >= level_value_by_name['WARN'] else False

        modules = self._determine_calling_module(by_module)

        # determine the default level
        current_threshold = self.log_config.get('default', 'WARN')

        # check for special logging levels in the configuration
        special_thresholds = self.log_config.get('modules', dict())

        # recurse into the special_levels configuration in order to determine the
        # deepest matching logging level.
        while modules and modules[0] in special_thresholds:
            module = modules.pop(0)
            special_thresholds = special_thresholds[module]
            # if the configuration defines a new threshold, use it
            current_threshold = special_thresholds.get('__threshold__', current_threshold)

        # if the level is higher than the threshold, we should log the call
        if level_value_by_name[current_threshold] <= level_value_by_name[level]:
            return True

        return False

    def _determine_calling_module(self, by_module):
        if not by_module:
            # if the caller did not specify a module that made the logging call, attempt to find
            # the module by inspecting the stack: record 0 is this, 1 is _should_log, 2 is the
            # logging function, and 3 will be the caller.
            record = inspect.stack()[3]
            # the first element of the record is the frame, which contains the locals and globals
            frame = record[0]
            f_globals = frame.f_globals

            # check the stack frame's globals for the __name__ attribute, which is the module name
            if '__name__' in f_globals:
                by_module = f_globals['__name__']
            else:
                # but it might not be a regular python module (such as a service.tac),
                # in which case we have to fall back to using the __file__ attribute.
                by_module = reflect.filenameToModuleName(f_globals['__file__'])

        elif not isinstance(by_module, basestring):
            # if the caller gave us an actual module, and not just its name, determine its
            # name and use it.
            by_module = reflect.fullyQualifiedName(by_module)
        
        modules = by_module.split('.')
        return modules

    def _msg(self, level, msg):
        # if we're not provided a message, assume there's an available exception
        if msg is Ellipsis:
            msg = failure.Failure()

        if isinstance(msg, failure.Failure):
            msg = msg.getTraceback()

        msg = '[%s] %s'%(level, msg)

        if reactor.running:
            log.msg(msg)
        else:
            print msg

    def debug(self, msg=Ellipsis):
        if self._should_log('DEBUG'):
            self._msg('DEBUG', msg)

    def info(self, msg=Ellipsis):
        if self._should_log('INFO'):
            self._msg('INFO', msg)

    def warn(self, msg=Ellipsis):
        if self._should_log('WARN'):
            self._msg('WARN', msg)

    def error(self, msg=Ellipsis):
        if self._should_log('ERROR'):
            self._msg('ERROR', msg)

    def critical(self, msg=Ellipsis):
        if self._should_log('CRITICAL'):
            self._msg('CRITICAL', msg)


def _set_global_logger(logger):
    global _logger, msg, debug, info, warn, error, critical
    _logger = logger
    debug = logger.debug
    info = logger.info
    warn = logger.warn
    error = logger.error
    critical = logger.critical


# default to using an non-configured DefaultLogger until something else is configured.
# the reason this is defined in-line instead of using _set_global_logger is to assist
# IDEs in recognizing the logging functions

_logger = DefaultLogger()
debug = _logger.debug
info = _logger.info
warn = _logger.warn
error = _logger.error
critical = _logger.critical


def configure(runtime_environment):
    # look up the name of the logger from the configuration
    log_config = runtime_environment.get_configuration_value('logging', dict())
    logger_name = log_config.get('logger', 'piped.log.DefaultLogger')

    # instantiate and configure the requested logger
    logger = reflect.namedAny(logger_name)()
    logger.configure(runtime_environment)

    _set_global_logger(logger)
