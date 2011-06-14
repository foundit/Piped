# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Utility processors that are useful in many contexts. """
from twisted.python import logfile
from zope import interface

from piped import util, processing
from piped.processors import base


class FileAppender(base.Processor):
    """ Append something to a file.

    The file defined by *file_path* is opened in append-mode.

    For every baton processed, the input found at *input_path* is
    passed through a *formatter*, which is defined by a lambda, in the
    same way eval-lambda is. The default formatter simply passes the
    input through to the format-string.

    The output of the formatter is then combined with *format*, which
    is a format string.

    Note that the output of the formatter need not be a string. It can
    e.g. be a dictionary, which is then converted to a string via the
    *format*-string.

    The result of combining the format with the output from the
    formatter is assumed to be a unicode-string, which is then encoded
    to a UTF8-bytestring before appending to the file.

    Note: The processor does not deal with flushing the buffers to
    disk.
    """
    interface.classProvides(processing.IProcessor)
    name = 'append-to-file'

    def __init__(self, file_path, input_path='', format=u'%s\n', formatter="input: input", namespace=None, encode='utf8', **kw):
        super(FileAppender, self).__init__(**kw)

        file_path = util.ensure_filepath(file_path)

        self.file = file_path.open('a')
        self.input_path = input_path

        self.formatter_definition = formatter
        self.namespace = namespace or dict()

        self.format = format
        self.encode = encode

    def configure(self, runtime_environment):
        self.formatter = util.create_lambda_function(self.formatter_definition, self=self, **self.namespace)

    def process(self, baton):
        input = util.dict_get_path(baton, self.input_path)
        if input:
            output = self.format % self.formatter(input)
            if self.encode:
                output = output.encode(self.encode)
            self.file.write(output)
        return baton


class LogAppender(FileAppender):
    """ Append something to a log. """
    interface.classProvides(processing.IProcessor)
    name = 'append-to-log'

    def __init__(self, file_path, **kw):
        super(LogAppender, self).__init__(file_path, **kw)

        file_path = util.ensure_filepath(file_path)

        self.file = logfile.DailyLogFile(file_path.basename(), file_path.dirname())
