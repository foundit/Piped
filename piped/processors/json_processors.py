# coding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Encoding and decoding JSON. """
from twisted.python import reflect
from zope import interface

from piped import util, processing
from piped.processors import base


try:
    import simplejson as json
except ImportError:
    import json


class JsonDecoder(base.InputOutputProcessor):
    name = 'decode-json'
    interface.classProvides(processing.IProcessor)

    def process_input(self, input, baton):
        return json.loads(input)


class JsonEncoder(base.InputOutputProcessor):
    """ In/Out-processor that encodes its input as JSON.

    Actually, it encodes the input with the configured *encoder*,
    which defaults to `piped.util.PipedJSONEncoder`. It expects the
    configured encoder to have an `encode(input)`-method.
    """
    name = 'encode-json'
    interface.classProvides(processing.IProcessor)

    # TODO: Should be a callback_path, optionally with a default callback.
    def __init__(self, encoder='piped.util.PipedJSONEncoder', **kw):
        super(JsonEncoder, self).__init__(**kw)
        self.json_encoder = reflect.namedAny(encoder)()

    def process_input(self, input, baton):
        return self.json_encoder.encode(input)


class JSONPEncoder(JsonEncoder):
    """ Encodes the input as JSON, wrapping the encoded object with
    `callback(...)`.

    The callback-value is retrieved via the *callback_path*, which
    defaults to `callback`.

    If *fallback_to_json* is true, then the result will be unwrapped
    JSON if no callback-string is found.
    """
    name = 'encode-jsonp'
    interface.classProvides(processing.IProcessor)

    def __init__(self, callback_path='callback', fallback_to_json=True, **kw):
        super(JSONPEncoder, self).__init__(**kw)
        self.callback_path = callback_path
        self.fallback_to_json = fallback_to_json

    def process_input(self, input, baton):
        input = super(JSONPEncoder, self).process_input(input, baton)
        callback = util.dict_get_path(baton, self.callback_path)
        if not callback:
            if self.fallback_to_json:
                return input
            else:
                raise ValueError('expected to find a callback')
        return '%s(%s)' % (callback.encode('ascii'), input)
