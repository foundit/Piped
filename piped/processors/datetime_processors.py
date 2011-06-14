# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime

from zope import interface

from piped import processing
from piped.processors import base


class DateTimeParser(base.InputOutputProcessor):
    """ Parses a timestamp according to *format_string*.

    If *as_date* is true, then a date-object is returned, instead of a datetime.
    """
    interface.classProvides(processing.IProcessor)
    name = 'parse-datetime'

    def __init__(self, format_string, as_date=False, **kw):
        super(DateTimeParser, self).__init__(**kw)
        self.format_string = format_string
        self.as_date = as_date

    def process_input(self, input, baton):
        dt = datetime.datetime.strptime(input, self.format_string)
        if self.as_date:
            return dt.date()
        return dt


class DateFormatter(base.InputOutputProcessor):
    """ Formats a date according to a format. """
    interface.classProvides(processing.IProcessor)
    name = 'format-date'

    def __init__(self, format_string, **kw):
        super(DateFormatter, self).__init__(**kw)
        self.format_string = format_string

    def process_input(self, input, baton):
        return input.strftime(self.format_string)
