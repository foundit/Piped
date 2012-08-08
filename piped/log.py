# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
""" Utilities for bootstrapping the logging system. """
import sys
import time

import yaml
import logging
import logging.config

from twisted.python import log, filepath
from zope import interface

import piped
from piped import util, yamlutil # used by the logging configuration to support our yaml extensions


class PythonLoggingObserver(log.PythonLoggingObserver):

    def __init__(self):
        self.loggers = dict()

    def emit(self, eventDict):
        if 'logLevel' in eventDict:
            level = eventDict['logLevel']
        elif eventDict['isError']:
            level = logging.ERROR
        else:
            level = logging.INFO
        text = log.textFromEventDict(eventDict)
        if text is None:
            return
        logger = self._get_logger(eventDict)
        logger.log(level, text)

    def _get_logger(self, eventDict):
        system = eventDict.get('system', 'twisted')

        if system == '-':
            system = 'twisted'

        if system not in self.loggers:
            self.loggers[system] = logging.getLogger(system)

        return self.loggers[system]


class UTCFormatter(logging.Formatter):
    converter = time.gmtime

    def formatTime(self, record, datefmt=None):
        if datefmt:
            # TODO: more custom formats
            datefmt = datefmt.replace('%03.f', '%03.f'%record.msecs)
        return super(UTCFormatter, self).formatTime(record, datefmt)


class REPLAwareUTCFormatter(UTCFormatter):
    """ A log formatter that is aware of a (possibly) running REPL.

    This is required because we set stdout to "raw" mode, which means we will need
    a few extra control sequences added to the console log output in order to preserve
    users' sanity.

    Additionally, we need this class to be in a different module than the rest of the
    REPL provider in order to see log messages from the REPL provider module.
    """
    #TODO: figure out why we can't even import the REPL provider here without losing
    #       log messages from the repl provider

    repl_provider = None
    repl_provider_module = 'piped.providers.repl_provider'
    repl_provider_class = 'PipedREPL'

    def format(self, record):
        if not self.repl_provider:
            self.__class__.repl_provider = sys.modules.get(self.repl_provider_module, None)

        formatted = super(REPLAwareUTCFormatter, self).format(record)
        if self.repl_provider and getattr(self.repl_provider, self.repl_provider_class).running:
            formatted = formatted.replace('\n', '\n\r\r') + '\r\r'
        return formatted


def configure(args):
    logging_config = filepath.FilePath(piped.__file__).sibling('logging.yaml')
    if args.logging_config:
        logging_config = util.ensure_filepath(yaml.load(args.logging_config))

    # TODO: support fileConfig (.ini-style) via fallback?
    log_config = yaml.load(logging_config.getContent())
    logging.config.dictConfig(log_config)


# instantiate a default logging observer that twistd can use.
observer = PythonLoggingObserver()