# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from piped_validation import processors
from twisted.python import reflect
from twisted.trial import unittest


class FakeSchema:

    def to_python(self, value):
        return dict(faked='schema')


class TestFormEncodeValidation(unittest.TestCase):

    def test_processor_invokes_the_schema(self):
        schema = reflect.fullyQualifiedName(FakeSchema)
        # We just check if the schema is invoked, we don't really care
        # about the validation here, since we're not unittesting
        # FormEncode.
        processor = processors.FormEncodeValidator(schema, 'input', 'output')
        baton = dict(input='disregarded')
        processor.process(baton)
        self.assertEquals(baton, dict(input='disregarded', output=dict(faked='schema')))
