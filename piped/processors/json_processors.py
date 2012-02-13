# coding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
""" Encoding and decoding JSON. """
import json

from twisted.python import reflect
from zope import interface

from piped import util, processing, yamlutil
from piped.processors import base


class JsonDecoder(base.InputOutputProcessor):
    """ Decodes JSON.

    The input may either be a string or a file-like object.
    """
    name = 'decode-json'
    interface.classProvides(processing.IProcessor)

    def __init__(self, decoder='json.JSONDecoder', **kw):
        """
        :param decoder: A fully qualified name of the :class:`json.JSONDecoder`.
        """
        super(JsonDecoder, self).__init__(**kw)
        self.json_decoder = reflect.namedAny(decoder)

    def process_input(self, input, baton):
        loader = json.loads
        if hasattr(input, 'read'):
            loader = json.load

        return loader(input, cls=self.json_decoder)


class JsonEncoder(base.InputOutputProcessor):
    """ Encodes JSON. """
    name = 'encode-json'
    interface.classProvides(processing.IProcessor)

    def __init__(self, encoder='piped.util.PipedJSONEncoder', indent=None, **kw):
        """
        :param encoder: A fully qualified name of the :class:`json.JSONEncoder`.

        :param indent: If ``indent`` is a non-negative integer, then
            JSON array elements and object members will be pretty-printed
            with that indent level. An indent level of 0 will only insert
            newlines. ``None`` is the most compact representation.
        """
        super(JsonEncoder, self).__init__(**kw)
        self.json_encoder = reflect.namedAny(encoder)
        self.indent = indent

    def process_input(self, input, baton):
        return json.dumps(input, cls=self.json_encoder, indent=self.indent)


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

    def __init__(self, callback=yamlutil.BatonPath('callback'), fallback_to_json=True, **kw):
        super(JSONPEncoder, self).__init__(**kw)
        self.callback = callback
        self.fallback_to_json = fallback_to_json

    def process_input(self, input, baton):
        input = super(JSONPEncoder, self).process_input(input, baton)
        callback = self.get_input(baton, self.callback, None)
        if isinstance(callback, (list, tuple)):
            callback = callback[0]
        if not callback:
            if self.fallback_to_json:
                return input
            else:
                raise ValueError('expected to find a callback')
        return '%s(%s)' % (callback.encode('ascii'), input)
