# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import abc
import types

from twisted.internet import defer

from piped import util, exceptions, yamlutil


class Processor(object):
    """ A base class used for many processors.

    This class provides a base implementation of many of the requirements of the
    `piped.processing.IProcessor` interface.

    The provides and depends-on facility enables the annotation of configuration anomalies, such as processors
    that require certain paths being set in incoming batons. It is not enforced.

    :ivar instance_provides: list of strings identifying keywords this processor provides for the pipeline.
    :ivar instance_depends_on: list of string identifying keywords this processor depends on from the pipeline.
    """
    __metaclass__ = abc.ABCMeta

    id = None
    evaluator = None
    processor_configuration = None

    #: list of strings identifying keywords processes of this class provides for the pipeline.
    provides = list()
    #: list of string identifying keywords processes of this class depends on from the pipeline.
    depends_on = list()

    def __init__(self, node_name=None):
        super(Processor, self).__init__()
        self.consumers = list()
        self.error_consumers = list()
        self.time_spent = 0
        self._node_name = node_name

    def get_input(self, baton, value, fallback=Ellipsis):
        """ Gets the actual input value.

        Example usage::

            def process(self, baton):
                foo = self.get_input(baton, self.foo)
                bar = self.get_input(baton, self.bar)

                return self.get_resulting_baton(baton, self.output_path, dict(foobar=foo*bar))

        :param baton: The baton that might contain the actual input value.
        :param value: An instance of :class:`~piped.yamlutil.BatonPath`, meaning the
            input value is found at the specified path in the baton, *or* any other
            value to be used as-is.
        :param fallback: The value to use if the input does not exist.
        """
        if isinstance(value, yamlutil.BatonPath):
            return util.dict_get_path(baton, value, fallback)
        return value

    def get_resulting_baton(self, baton, path, value):
        """ Returns the new baton after optionally setting an output value.

        Example usage::

            def process(self, baton):
                value = self._calculate(baton)
                return self.get_resulting_baton(baton, self.output_path, value)

        :param baton: The baton in which the output value may be set.
        :param path: The path in the baton to set the value. Some paths have special meanings:

            None:
                The value should be discarded and the baton returned unchanged.
            An empty string:
                The value should completely replace the baton.
        
        :return: The new baton.
        """
        if path is None:
            return baton
        elif path == '':
            return value

        util.dict_set_path(baton, path, value)
        return baton

    @property
    def instance_depends_on(self):
        return list()

    @property
    def instance_provides(self):
        return list()
    
    @property
    def node_name(self):
        return self._node_name or self.name

    @abc.abstractmethod
    def process(self, baton):
        """ Process a baton. """
        raise NotImplementedError()

    @abc.abstractproperty
    def name(self):
        """ The name of the processor. """

    def configure(self, runtime_environment):
        """ Configures this `Processor` with the runtime environment. """

    def get_consumers(self, baton):
        """ Return a list of consumers of this processor. """
        return self.consumers

    def get_error_consumers(self, baton):
        """ Return a list of error consumers of this processor. """
        return self.error_consumers


class InputOutputProcessor(Processor):
    """ A processor that processes some input and returns some output.

    path
        A path is a string that describes where an input or output value is found
        in the baton. This is resolved by using :func:`piped.util.dict_get_path`
        and :func:`piped.util.dict_set_path`, respectively.

    """

    def __init__(self, input_path='', output_path=Ellipsis, skip_if_nonexistent=True, input_fallback=None, *a, **kw):
        """
        :param input_path: A path to the input.
        :param output_path: A path to the output. If not specified, it becomes
            the same as the input_path, meaning the processed output will replace
            the input. A value of ``None`` (``null`` in JSON) means that the
            output should be discarded.
        :param skip_if_nonexistent: Whether to skip the processor if the input
            is not found in the baton.
        :param input_fallback: The value to use if the input is not found and
            skip_if_nonexistent is false.

        """
        super(InputOutputProcessor, self).__init__(*a, **kw)
        self.input_path = input_path

        self.output_path = output_path
        if self.output_path == Ellipsis:
            self.output_path = self.input_path

        self.skip_if_nonexistent = skip_if_nonexistent
        self.input_fallback = input_fallback

    @property
    def instance_depends_on(self):
        """ Return a list of keywords this instance depends on.

        By default, this processor depends on the input path being provided if ``skip_if_nonexistent`` is ``True``.
        """
        if not self.skip_if_nonexistent:
            return [self.input_path]
        return list()

    @property
    def instance_provides(self):
        """ Return a list of keyword this instance provides.

        If the output_path is not ``None``, this instance provides its output_path.
        """
        if self.output_path:
            return [self.output_path]
        return list()

    @defer.inlineCallbacks
    def process(self, baton):
        input = util.dict_get_path(baton, self.input_path, Ellipsis)

        if input is Ellipsis:
            if self.skip_if_nonexistent:
                defer.returnValue(baton)
            input = self.input_fallback

        output = yield self.process_input(input=input, baton=baton)

        if self.output_path == '':
            defer.returnValue(output) # the output replaces the baton

        if self.output_path is None:
            defer.returnValue(baton) # discard the output

        util.dict_set_path(baton, self.output_path, output)
        defer.returnValue(baton)

    @abc.abstractmethod
    def process_input(self, input, baton):
        """ This is called by :func:`process` when a baton with a valid input
        value is being processed.

        :param baton: The baton being processed.
        :param input: The input that were found in the baton.
        """


class MappingProcessor(Processor):
    """ A processor that processes multiple inputs and produces multiple outputs. """

    def __init__(self, mapping, input_path_prefix='', output_path_prefix='', skip_if_nonexistent=True, input_fallback=None, *a, **kw):
        """
        :param mapping:
            The mapping represents a list of input and output paths and any
            additional configuration they mapping entry requires.

            The mapping may be a list of *map entries*, a dict that maps
            input paths to a *map entry*.

            A *map entry* specifies the input_path, the output_path and any
            additional configuration options for the map entry. If no output_path
            is specified, it defaults to being the same as input_path. The
            input_path should be skipped if the map entry is part of a input_path
            -> *map entry* dict.

            A *map entry* may be a string, in which case it is rewritten to
            a dict(input_path=map_entry)

            Additional configuration options are passed to the
            :func:`validate_additional_kwargs` function during initialization,
            and which subclasses are highly encouraged to override and perform
            any necessary validation in. These will be passed as keyword arguments
            to :func:`process_mapping` along with the input.

            Example mappings::

                foo: foo
                bar:
                    an_option: test
                baz:
                    output_path: zip

            This is equivalent with::

                - foo
                - bar:
                    an_option: test
                - baz:
                    output_path: zip

            And also::

                - foo
                - bar:
                    an_option: test
                - baz: zip


        :param input_path_prefix: Prefix used on all input paths.

        :param output_path_prefix: Prefix used on all output paths. If an
            output_path for a *map entry* is ``None``, however, the prefix is
            not used.

        :param skip_if_nonexistent: Whether to skip the processor for a single
            map entry if the input is not found in the baton.

        :param input_fallback: The value to use if the input is not found and
            skip_if_nonexistent is false.

        """
        super(MappingProcessor, self).__init__(*a, **kw)
        self.input_prefix = input_path_prefix
        self.output_prefix = output_path_prefix

        self.mapping = self._get_mapping_with_prefixes(mapping)

        self.skip_if_nonexistent = skip_if_nonexistent
        self.input_fallback = input_fallback

    def _get_mapping_with_prefixes(self, mapping):
        mapping_with_prefixes = list()
        if isinstance(mapping, list):
            # this is a list of configurations
            for map_entry in mapping:
                prefixed_map_entry = self._get_prefixed_map_entry_for_map_entry(map_entry)
                mapping_with_prefixes.append(prefixed_map_entry)

            return mapping_with_prefixes

        for input_path, map_entry in mapping.items():
            prefixed_map_entry = self._get_prefixed_map_entry_for_map_entry({input_path: map_entry})
            mapping_with_prefixes.append(prefixed_map_entry)

        return mapping_with_prefixes

    def _get_prefixed_map_entry_for_map_entry(self, map_entry):
        # if the user has specified a simple string, use it as the path.
        if isinstance(map_entry, basestring):
            map_entry = dict(input_path=map_entry)

        input_path = map_entry.get('input_path', Ellipsis)

        if len(map_entry) == 1 and input_path is Ellipsis:
            # if we only have one key and we found no 'input_path' in the map_entry,
            # we assume that is the input_path, and the value defines the output or any extra options:
            only_key = map_entry.keys()[0]
            map_entry = map_entry.values()[0]

            # if the user specified a mapping on the format dict(output_path='key'), this guards
            # against setting input_path = 'output_path'
            if only_key != 'output_path':
                input_path = only_key

            if isinstance(map_entry, (basestring, types.NoneType)):
                # this was a simple dict(foo='destination') mapping
                map_entry = dict(output_path=map_entry)

            # if the nested dictionary tries to define an input_path, we cannot know
            # which one the user meant.
            if 'input_path' in map_entry and map_entry['input_path'] != input_path:
                e_msg = 'Invalid mapping configuration.'
                detail = 'Input path was defined twice. Once as %r, then again as %r.' % (input_path, map_entry['input_path'])
                raise exceptions.ConfigurationError(e_msg, detail)

        if input_path is Ellipsis:
            e_msg = 'Invalid mapping configuration.'
            detail = 'Could not find an input_path for: %r' % map_entry
            raise exceptions.ConfigurationError(e_msg, detail)

        output_path = map_entry.get('output_path', input_path)

        prefixed_map_entry = self._create_and_validate_prefixed_map_entry(input_path, output_path, map_entry)
        return prefixed_map_entry

    def _create_and_validate_prefixed_map_entry(self, input_path, output_path, additional_kwargs):
        prefixed = dict()
        prefixed['input_path'] = self.input_prefix + input_path
        prefixed['output_path'] = None
        if output_path is not None:
            prefixed['output_path'] = self.output_prefix + output_path

        for path in 'input_path', 'output_path':
            # it may be None... but if it isn't, it should not end with a "."
            if prefixed[path] and prefixed[path].endswith('.'):
                e_msg = 'Invalid paths in mapping configuration.'
                detail = 'Paths cannot end with a ".". The specified paths were: %r' % prefixed
                raise exceptions.ConfigurationError(e_msg, detail)

            # strip all path-related keys from the configuration, leaving only additional
            # configuration options
            additional_kwargs.pop(path, None)


        # let subclasses validate or update the extra configuration options.
        self.validate_additional_kwargs(input_path, output_path, **additional_kwargs)

        prefixed['additional_kwargs'] = additional_kwargs
        return prefixed

    def validate_additional_kwargs(self, input_path, output_path, **additional_kwargs):
        """ A hook for subclassing `Processor`\s to validate or rewrite the contents of ``extra_config``. """

    @property
    def instance_depends_on(self):
        """ Return a list of keywords this instance depends on.

        By default, this processor depends on all the input paths being provided if ``skip_if_nonexistent is True``
        """
        if not self.skip_if_nonexistent:
            return [map_entry['input_path'] for map_entry in self.mapping if map_entry['input_path']]
        return list()

    @property
    def instance_provides(self):
        """ Return a list of keyword this instance provides.

        By default, we provide all our output paths.
        """
        return [map_entry['output_path'] for map_entry in self.mapping if map_entry['output_path']]

    @defer.inlineCallbacks
    def process(self, baton):
        """ Processes a baton.

        This function will call :func:`process_mapping` with the input for each input in the mapping.
        """
        for map_entry in self.mapping:
            input_path = map_entry['input_path']
            output_path = map_entry['output_path']
            additional_kwargs = map_entry['additional_kwargs']

            input = self.get_input(baton, input_path, **additional_kwargs)

            if input is Ellipsis:
                if self.skip_if_nonexistent:
                    continue
                # TODO: per-entry fallback
                input = self.input_fallback

            output = yield self.process_mapping(input=input, input_path=input_path, output_path=output_path,
                                                baton=baton, **additional_kwargs)

            if output_path == '':
                baton = output
                continue

            if output_path is None:
                continue # discard the output

            self.set_output(baton, output, output_path, **additional_kwargs)

        defer.returnValue(baton)

    def get_input(self, baton, input_path, **kwargs):
        """ Return the appropriate input for the given input_path and baton. """
        return util.dict_get_path(baton, input_path, Ellipsis)

    def set_output(self, baton, output, output_path, **kwargs):
        """ Set the output. """
        util.dict_set_path(baton, output_path, output)

    @abc.abstractmethod
    def process_mapping(self, input, input_path, output_path, baton, **additional_kwargs):
        """ This function will be called once by :func:`process` for each input.

        :param baton: The baton being processed.

        :param input: The current input. This is the argument most processors will
            operate on.

        :param additional_kwargs: the additional kwargs for this input/output_path
            map entry that were declared and validated during the initialization of
            this `Processor`
        """
        raise NotImplementedError()
