# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json

from twisted.internet import defer
from twisted.python import reflect
from twisted.trial import unittest

from piped.processors import json_processors


class JsonDecoderTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_simple_decoding(self):
        processor = json_processors.JsonDecoder()
        baton = yield processor.process(json.dumps(dict(foo=42)))
        self.assertEquals(baton, dict(foo=42))


class StubEncoder(object):

    def encode(self, whatever):
        return 'custom encoder'


class JsonEncoderTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_simple_encoding(self):
        expected_result = json.dumps(dict(foo=42))
        processor = json_processors.JsonEncoder()
        baton = yield processor.process(dict(foo=42))
        self.assertEquals(baton, expected_result)

    @defer.inlineCallbacks
    def test_encoding_with_custom_encoder(self):
        processor = json_processors.JsonEncoder(encoder=reflect.fullyQualifiedName(StubEncoder))
        expected_result = 'custom encoder'
        baton = yield processor.process(dict(foo=42))
        self.assertEquals(baton, expected_result)


class JSONPEncoderTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_encoding_with_jsonp_callback(self):
        expected_result = 'a_callback(%s)' % json.dumps(dict(foo=42))
        processor = json_processors.JSONPEncoder(input_path='to_encode', output_path='')
        baton = yield processor.process(dict(to_encode=dict(foo=42), callback='a_callback'))
        self.assertEquals(baton, expected_result)

    @defer.inlineCallbacks
    def test_encoding_with_custom_callback(self):
        expected_result = 'other_callback(%s)' % json.dumps(dict(foo=42))
        processor = json_processors.JSONPEncoder(input_path='to_encode', output_path='')
        baton = yield processor.process(dict(to_encode=dict(foo=42), callback='other_callback'))
        self.assertEquals(baton, expected_result)

    @defer.inlineCallbacks
    def test_encoding_with_no_callback(self):
        expected_result = json.dumps(dict(foo=42))
        processor = json_processors.JSONPEncoder(input_path='to_encode', output_path='')
        baton = yield processor.process(dict(to_encode=dict(foo=42)))
        self.assertEquals(baton, expected_result)

    def test_encoding_with_no_callback_fails_when_it_should(self):
        expected_result = json.dumps(dict(foo=42))
        processor = json_processors.JSONPEncoder(input_path='to_encode', fallback_to_json=False)
        return self.assertFailure(processor.process(dict(to_encode=dict(foo=42))), ValueError)
