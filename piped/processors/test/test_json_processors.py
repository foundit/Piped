# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json
import StringIO

from twisted.internet import defer
from twisted.python import reflect
from twisted.trial import unittest

from piped.processors import json_processors


class StubDecoder(json.JSONDecoder):
    """ A custom decoder that doubles any decoded values. """
    def __init__(self, **kwargs):
        kwargs['object_hook'] = self.dict_to_object
        super(StubDecoder, self).__init__(**kwargs)

    def dict_to_object(self, dict_like):
        return dict([(key, value*2) for key, value in dict_like.items()])


class StubEncoder(json.JSONEncoder):

    def encode(self, o):
        return 'custom encoder'


class JsonDecoderTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_simple_decoding(self):
        processor = json_processors.JsonDecoder()
        baton = yield processor.process(json.dumps(dict(foo=42)))
        self.assertEquals(baton, dict(foo=42))

    @defer.inlineCallbacks
    def test_decoding_buffer(self):
        processor = json_processors.JsonDecoder()
        baton = yield processor.process(StringIO.StringIO(json.dumps(dict(foo=42))))
        self.assertEquals(baton, dict(foo=42))

    @defer.inlineCallbacks
    def test_custom_decoder(self):
        processor = json_processors.JsonDecoder(decoder=reflect.fullyQualifiedName(StubDecoder))
        baton = yield processor.process(json.dumps(dict(foo=42)))
        self.assertEquals(baton, dict(foo=84))


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

    @defer.inlineCallbacks
    def test_pretty_encoding(self):
        expected_result = json.dumps(dict(foo=42), indent=4)
        processor = json_processors.JsonEncoder(indent=4)
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
