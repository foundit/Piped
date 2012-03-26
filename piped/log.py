# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
""" Utilities for bootstrapping the logging system. """
import time

import yaml
import logging
import logging.config

from twisted.python import log, filepath
from zope import interface

import piped


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


def configure(args):
    logging_config = filepath.FilePath(piped.__file__).sibling('logging.yaml')
    if args.logging_config:
        logging_config = filepath.FilePath(args.logging_config)

    # TODO: support fileConfig (.ini-style) via fallback?
    log_config = yaml.load(logging_config.getContent())
    logging.config.dictConfig(log_config)


# instantiate a default logging observer that twistd can use.
observer = PythonLoggingObserver()