# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.trial import unittest

from piped import log

class TestPythonLoggingObserver(unittest.TestCase):

    def setUp(self):
        self.observer = log.PythonLoggingObserver()

    def test_system_logger(self):
        self.observer.emit(dict(system='-', isError=False, message='test'))
        self.assertIn('twisted', self.observer.loggers)

    def test_comma_in_logger_name(self):
        self.observer.emit(dict(system='foo,bar', isError=False, message='test'))
        self.observer.emit(dict(system='foo,baz', isError=False, message='test'))

        self.assertIn('foo', self.observer.loggers)
        self.assertEqual(1, len(self.observer.loggers))