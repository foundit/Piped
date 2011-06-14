# encoding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json

from twisted.trial import unittest

from piped import processing
from piped.processors import file_processors


try:
    import cStringIO as StringIO
except ImportError:
    import StringIO


class StubFilePath(object):
    def __init__(self):
        self.fake_file = StringIO.StringIO()
    def open(self, mode='r'):
        return self.fake_file


class TestFileAppender(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()

    def make_fake_file_and_path(self):
        fake_filepath = StubFilePath()
        return fake_filepath.fake_file, fake_filepath

    def assertLinesEqual(self, buffer, expected_lines):
        buffer.seek(0)
        self.assertEquals(buffer.readlines(), expected_lines)

    def test_appending_to_the_file(self):
        # Set up the processor to append to a buffer.
        fake_file, fake_file_path = self.make_fake_file_and_path()

        processor = file_processors.FileAppender(fake_file_path)
        processor.configure(self.runtime_environment)
        processor.process(u'foo-æøå')

        self.assertLinesEqual(fake_file, ['foo-æøå\n'])

    def test_appending_json_encoded_inputs(self):
        # Set up the processor to append to a buffer.
        fake_file, fake_file_path = self.make_fake_file_and_path()

        namespace = dict(json='json')

        processor = file_processors.FileAppender(fake_file_path, input_path='input',
                                                formatter='input: json.dumps(input)',
                                                namespace=namespace)
        processor.configure(self.runtime_environment)
        processor.process(dict(input=dict(foo='one\ntwo')))
        processor.process(dict(input=dict(apple=u'¡some unicode!')))

        expected_lines = [json.dumps(dict(foo='one\ntwo')) + '\n',
                          json.dumps(dict(apple=u'¡some unicode!')) + '\n']
        self.assertLinesEqual(fake_file, expected_lines)

    def test_appending_with_custom_format_and_formatter(self):
        # Set up the processor to append to a buffer.
        fake_file, fake_file_path = self.make_fake_file_and_path()

        processor = file_processors.FileAppender(fake_file_path, format=u'%(foo)s %(bar)s\n',
                                                formatter='input: input')
        processor.configure(self.runtime_environment)
        processor.process(dict(foo=u'¡foo!', bar='bar', baz='ignored'))
        processor.process(dict(bar=42, foo=123))

        expected_lines = ['¡foo! bar\n', '123 42\n']
        self.assertLinesEqual(fake_file, expected_lines)
