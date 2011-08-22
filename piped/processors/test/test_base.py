from twisted.internet import defer
from twisted.trial import unittest

from piped import exceptions, yamlutil
from piped.processors import base


class StubProcessor(base.Processor):
    name = 'test'

    def process(self, baton):
        return baton


class ProcessorTest(unittest.TestCase):

    def test_get_input_from_config(self):
        processor = StubProcessor()
        baton = dict(foo='foo_value')

        # since we're not using a yamlutil.BatonPath to get the input, we should
        # get the value echoed back to us:
        input = processor.get_input(baton, 'foo')
        self.assertEquals(input, 'foo')

    def test_get_input_from_baton(self):
        processor = StubProcessor()
        baton = dict(foo='foo_value')

        # since we're using a yamlutil.BatonPath to get the input, the input should come from the baton.
        input = processor.get_input(baton, yamlutil.BatonPath('foo'))
        self.assertEquals(input, 'foo_value')

        # if the path does not exist, we expect to get the fallback
        input = processor.get_input(dict(), yamlutil.BatonPath('foo'))
        self.assertEquals(input, Ellipsis)

    def test_get_resulting_baton(self):
        processor = StubProcessor()

        # if the path is a string, set the output on that path.
        baton = processor.get_resulting_baton(dict(foo='foo_value'), path='bar.baz', value='baz_value')
        self.assertEquals(baton, dict(foo='foo_value', bar=dict(baz='baz_value')))

        # if the path is None, ignore the output
        baton = processor.get_resulting_baton(dict(foo='foo_value'), path=None, value='ignored_value')
        self.assertEquals(baton, dict(foo='foo_value'))

        # if the path is the empty string, it should replace the baton
        baton = processor.get_resulting_baton(dict(), path='', value='the new baton')
        self.assertEquals(baton, 'the new baton')


class StubInputOutputProcessor(base.InputOutputProcessor):
    """ An InputOutputProcessor that defers the processing to a function. """
    name = 'test'
    def __init__(self, process_input_func, *a, **kw):
        super(StubInputOutputProcessor, self).__init__(*a, **kw)
        self.processor = process_input_func

    def process_input(self, *a, **kw):
        return self.processor(*a, **kw)


class InputOutputProcessorTest(unittest.TestCase):
    def get_processor(self, process_input, **config):
        processor = StubInputOutputProcessor(process_input, **config)
        return processor

    @defer.inlineCallbacks
    def test_basic_input_exists(self):
        inputs = list()

        def process(input, baton):
            inputs.append(input)
            return input

        processor = self.get_processor(process)

        result = yield processor.process('test')
        self.assertEquals(inputs, ['test'])
        self.assertEquals(result, 'test')

    @defer.inlineCallbacks
    def test_inputs_skip(self):
        inputs = list()

        def process(input, baton):
            inputs.append(input)
            return input

        processor = self.get_processor(process, input_path='foo')
        result = yield processor.process(dict(text='test'))

        # this should not be processed, since the input_path does not exist
        self.assertEquals(inputs, [])
        # but the baton should be unchanged.
        self.assertEquals(result, dict(text='test'))

    @defer.inlineCallbacks
    def test_inputs_noskip_and_fallback(self):
        inputs = list()

        def process(input, baton):
            inputs.append(input)
            return input

        processor = self.get_processor(process, input_path='foo', skip_if_nonexistent=False, input_fallback='fallback')
        result = yield processor.process(dict(text='test'))

        # will get the fallback
        self.assertEquals(inputs, ['fallback'])
        self.assertEquals(result, dict(text='test', foo='fallback'))

    @defer.inlineCallbacks
    def test_output_setting_implicit_output_path(self):
        processor = self.get_processor(lambda input, baton: 'replaces baton', input_path='')
        result = yield processor.process(dict(text='test'))
        self.assertEquals(result, 'replaces baton')

    @defer.inlineCallbacks
    def test_output_setting_implicit_output_path_with_setitem(self):
        processor = self.get_processor(lambda input, baton: 'changes baton: %s'%input, input_path='text')
        result = yield processor.process(dict(text='test'))
        self.assertEquals(result, dict(text='changes baton: test'))

    @defer.inlineCallbacks
    def test_output_setting_explicit_output_path(self):
        processor = self.get_processor(lambda input, baton: 'adds to baton', input_path='', output_path='bar')
        result = yield processor.process(dict(text='test'))
        self.assertEquals(result, dict(text='test', bar='adds to baton'))

    @defer.inlineCallbacks
    def test_output_setting_explicit_overwriting(self):
        processor = self.get_processor(lambda input, baton: 'changes baton: %s'%input, input_path='text', output_path='text')
        result = yield processor.process(dict(text='test'))
        self.assertEquals(result, dict(text='changes baton: test'))

    @defer.inlineCallbacks
    def test_output_setting_discarding(self):
        inputs = list()
        processor = self.get_processor(lambda input, baton: inputs.append(input), input_path='text', output_path=None)
        result = yield processor.process(dict(text='test'))
        # it was processed, but had no effect:
        self.assertEquals(result, dict(text='test'))
        self.assertEquals(inputs, ['test'])


class StubMappingProcessor(base.MappingProcessor):
    name = 'test-mapper'
    def __init__(self, mapper, *a, **kw):
        super(StubMappingProcessor, self).__init__(*a, **kw)
        self.mapper = mapper

    def process_mapping(self, *a, **kw):
        return self.mapper(*a, **kw)


class MappingProcessorTest(unittest.TestCase):
    def get_processor(self, mapper, **config):
        processor = StubMappingProcessor(mapper, **config)
        return processor

    @defer.inlineCallbacks
    def test_mapper(self):
        expected_inputs = dict(
            foo=('foo-value', dict()),
            bar=('bar-value', dict(test=True)),
        )

        configurations = [
            [ # the mapping may be a list of explicit configs
              dict(input_path='foo', output_path='foo'),
              dict(input_path='bar', output_path='bar', test=True)
            ],
            [ # the output_paths will be the input_path by default
              dict(input_path='foo'),
              dict(input_path='bar', test=True)
            ],
            [ # the input_paths may come from the key in a mapping
              dict(foo='foo'), # this is explicit about output_path='foo'
              dict(bar=dict(test=True))
            ],
            dict(
                foo=dict(),
                bar=dict(test=True)
            ),
            dict( # be a string... or even defined twice, as long as they are the same (bar)
                foo='foo',
                bar=dict(input_path='bar', test=True)
            ),
            [ # even a list of strings is ok:
              'foo',
              dict(bar=dict(test=True)) # but bar requires this kwarg to be specified
            ]
        ]

        for configuration in configurations:
            inputs = dict()

            processor = self.get_processor(lambda input, baton, input_path, output_path, **kw: inputs.__setitem__(input_path, (input,kw)), mapping=configuration)

            result = yield processor.process(dict(foo='foo-value', bar='bar-value'))
            self.assertEquals(inputs, expected_inputs)
            self.assertEquals(result, dict(foo=None, bar=None)) # since __setitem__ returns None

    @defer.inlineCallbacks
    def test_mapping_to_root_baton(self):
        mapping = {
            'foo.bar.baz': ''
        }

        processor = self.get_processor(lambda input, baton, *a, **kw: input, mapping=mapping)

        result = yield processor.process(dict())
        self.assertEquals(result, dict())

        result = yield processor.process(dict(foo=dict(bar=dict(baz='this will become the new baton'))))
        self.assertEquals(result, 'this will become the new baton')

    @defer.inlineCallbacks
    def test_mapping_with_fallback(self):
        mapping = [
            'foo.bar', 'zip'
        ]

        processor = self.get_processor(lambda input, baton, *a, **kw: input, mapping=mapping, skip_if_nonexistent=False, input_fallback='fallback')
        result = yield processor.process(dict())
        self.assertEquals(result, dict(zip='fallback', foo=dict(bar='fallback')))

    @defer.inlineCallbacks
    def test_mapping_from_root_baton(self):
        mapping = {
            '': 'foo.bar'
        }

        processor = self.get_processor(lambda input, baton, *a, **kw: input, mapping=mapping)

        baton = dict(test='value')
        result = yield processor.process(baton)

        self.assertEquals(result['test'], 'value') # input should remain in the baton
        self.assertEquals(result['foo']['bar'], result) # this mapping causes a recursion

    def test_invalid_configurations(self):
        mapping = dict(
            foo=dict(input_path='bar')
        )

        # input path defined twice
        self.assertRaises(exceptions.ConfigurationError, self.get_processor, lambda *a, **kw: None, mapping=mapping)

        mapping = [
            dict(output_path='anything')
        ]
        # input path is missing
        self.assertRaises(exceptions.ConfigurationError, self.get_processor, lambda *a, **kw: None, mapping=mapping)

        mapping = [
            dict(input_path='foo', output_path='')
        ]
        # output_path_prefix+output_path will end with a '.'
        self.assertRaises(exceptions.ConfigurationError, self.get_processor, lambda *a, **kw: None, mapping=mapping, output_path_prefix='bar.')

    @defer.inlineCallbacks
    def test_discard_the_output(self):
        mapping = {
            'foo': None
        }
        inputs = list()

        processor = self.get_processor(lambda input, baton, *a, **kw: inputs.append(input), mapping=mapping)
        baton = dict(foo='value')
        result = yield processor.process(baton)

        self.assertEquals(result, dict(foo='value')) # the output should be discarded, leaving the baton unchanged
        self.assertEquals(inputs, ['value']) # the output
