# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.trial import unittest

from piped import log, util

class LogTest(unittest.TestCase):
    def setUp(self):
        self.modules = dict()
        self.log_config = dict(default='ERROR', modules=self.modules)
        self.logger = log.DefaultLogger()
        self.logger.log_config = self.log_config

    def _set_threshold(self, module_name, threshold):
        """ Sets a special loglevel for the given module. """
        util.dict_set_path(self.modules, module_name, dict(__threshold__=threshold))

    def test_defaults(self):
        self.assertEquals(self.logger._should_log('ERROR', 'foo'), True)
        self.assertEquals(self.logger._should_log('WARN', 'foo'), False)

    def test_fallthrough_with_another_module_having_specials(self):
        self._set_threshold('bar', 'WARN')

        self.assertEquals(self.logger._should_log('ERROR', 'foo'), True)
        self.assertEquals(self.logger._should_log('WARN', 'foo'), False)

    def test_special_threshold(self):
        self._set_threshold('foo', 'WARN')

        self.assertEquals(self.logger._should_log('ERROR', 'foo'), True)
        self.assertEquals(self.logger._should_log('WARN', 'foo'), True)
        self.assertEquals(self.logger._should_log('INFO', 'foo'), False)

    def test_inherited_threshold(self):
        self._set_threshold('foo', 'WARN')

        self.assertEquals(self.logger._should_log('ERROR', 'foo.bar'), True)
        self.assertEquals(self.logger._should_log('WARN', 'foo.bar'), True)
        self.assertEquals(self.logger._should_log('INFO', 'foo.bar'), False)

    def test_logger_levels_with_implicit_module(self):
        self._set_threshold(__name__, 'WARN')

        calls = list()
        def _msg(level, msg):
            self.assertEquals(msg, 'hello')
            calls.append(level)

        self.logger._msg = _msg

        # the logger will inspect the stack and find out that the logging calls come
        # from this test module
        for level in log.level_value_by_name:
            getattr(self.logger, level.lower())('hello')

        self.assertEquals(sorted(calls), sorted(['WARN', 'ERROR', 'CRITICAL']))
