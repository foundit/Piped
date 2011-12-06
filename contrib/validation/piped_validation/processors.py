# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.python import reflect
from zope import interface

from piped import util, processing
from piped.processors import base


class FormEncodeValidator(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'validate-with-formencode'

    def __init__(self, schema, input_path, output_path, **kw):
        super(FormEncodeValidator, self).__init__(**kw)
        self.schema = reflect.namedAny(schema)()
        self.input_path = input_path
        self.output_path = output_path

    def process(self, baton):
        input = util.dict_get_path(baton, self.input_path)
        util.dict_set_path(baton, self.output_path, self.schema.to_python(input))
        return baton
