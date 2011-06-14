# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime

from twisted.trial import unittest
from twisted.internet import defer

from piped.processors import datetime_processors


class DateTimeParserTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_parsing_and_getting_date(self):
        processor = datetime_processors.DateTimeParser(format_string='%Y-%m-%dT%H:%M:%S', as_date=True)
        result = yield processor.process('2011-02-03T15:14:12')
        self.assertEquals(result, datetime.date(2011, 2, 3))

    @defer.inlineCallbacks
    def test_parse_with_different_formats(self):
        simple_tests = dict()
        simple_tests[('2011-02-03', '%Y-%m-%d')] = datetime.datetime(2011, 2, 3)
        simple_tests[('2011-2-3', '%Y-%m-%d')] = datetime.datetime(2011, 2, 3)
        simple_tests[('2011-2-3-15-14-12', '%Y-%m-%d-%H-%M-%S')] = datetime.datetime(2011, 2, 3, 15, 14, 12)

        for (datetime_string, format_string), expected_datetime in simple_tests.items():
            processor = datetime_processors.DateTimeParser(format_string=format_string)
            result = yield processor.process(datetime_string)
            self.assertEquals(expected_datetime, result)

        processor = datetime_processors.DateTimeParser(format_string='%Y')
        self.assertRaises(ValueError, processor.process_input, 'this string does not match the format', baton=None)


class DateFormatterTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_parse_simple(self):
        simple_tests = dict()
        simple_tests[(datetime.datetime(2011, 2, 3), '%Y-%m-%d')] = '2011-02-03'
        simple_tests[(datetime.datetime(2011, 2, 3, 15, 14, 12), '%Y-%m-%d-%H-%M-%S')] = '2011-02-03-15-14-12'

        simple_tests[(datetime.date(2011, 2, 3), '%Y-%m-%d')] = '2011-02-03'

        for (date_like, format_string), expected_result in simple_tests.items():
            processor = datetime_processors.DateFormatter(format_string=format_string)
            result = yield processor.process(date_like)
            self.assertEquals(expected_result, result)

        # invalid format string
        processor = datetime_processors.DateFormatter(format_string='%FAIL%')
        self.assertRaises(ValueError, processor.process_input, datetime.datetime.now(), baton=None)
