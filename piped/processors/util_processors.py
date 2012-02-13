# -*- test-case-name: piped.processing.test.test_utilprocessors -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Utility processors that are useful in many contexts. """
import collections
import copy
import pdb
import pprint
import sys
import tempfile

from twisted.application import service
from twisted.internet import defer, reactor
from twisted.python import failure, reflect
from zope import interface

from piped import exceptions, util, log, processing, yamlutil
from piped.processors import base


class MergeWithDictProcessor(base.Processor):
    """ Processor that merges the baton with the provided dictionary.

    Expects a kw-argument "dict", which is the dictionary to merge.
    """
    name = 'merge-with-dict'
    interface.classProvides(processing.IProcessor)

    def __init__(self, dict=None, merge_args=dict(), **kw):
        super(MergeWithDictProcessor, self).__init__(**kw)
        self.dict = dict
        if dict is None:
            self.dict = dict()
        self.merge_args = merge_args
        # Defaulting to true because most of the time we expect the
        # dictionary we pass into a pipeline to be the same dictionary
        # *instance* that is sent to every processor.
        merge_args.setdefault('inline', True)

    def process(self, baton):
        return util.merge_dicts(baton, self.dict, **self.merge_args)


class CallNamedAny(base.Processor):
    """ Calls a named anything. """
    interface.classProvides(processing.IProcessor)
    name = 'call-named-any'

    def __init__(self, name, output_path='result', args=tuple(), kwargs=None, **kw):
        super(CallNamedAny, self).__init__(**kw)

        self.name = name
        self.output_path = output_path
        self.args = args
        self.kwargs = kwargs or dict()

    def process(self, baton):
        any = reflect.namedAny(self.name)

        result = any(*self.args, **self.kwargs)

        if self.output_path == '':
            return result

        util.dict_set_path(baton, self.output_path, result)

        return baton


class CallbackDeferred(base.Processor):
    """ Callbacks a deferred. """
    interface.classProvides(processing.IProcessor)
    name = 'callback-deferred'

    def __init__(self, deferred_path='deferred', result=Ellipsis, result_path='result', **kw):
        """

        :param deferred_path: The path to the deferred inside the baton.

        :param result: The result used to callback the deferred. This takes precedence
            over ``result_path``

        :param result_path: A path to the result in the baton.
        """
        super(CallbackDeferred, self).__init__(**kw)

        self.result_path = result_path
        self.result = result

        self.deferred_path = deferred_path

    def process(self, baton):
        deferred = util.dict_get_path(baton, self.deferred_path)

        result = self.result
        if result is Ellipsis:
            result = util.dict_get_path(baton, self.result_path)

        deferred.callback(result)

        return baton


class Shutdown(base.Processor):
    """ Stops the piped process. """
    interface.classProvides(processing.IProcessor)
    name = 'shutdown'

    def configure(self, runtime_environment):
        self.application = runtime_environment.application

    @defer.inlineCallbacks
    def process(self, baton):
        yield service.IService(self.application).stopService()
        yield reactor.stop()

        defer.returnValue(baton)


class CoroutineWrapper(base.Processor):
    """ Passes batons to the wrapped coroutine. """
    name = 'wrap-coroutine'

    def __init__(self, coroutine, **kw):
        super(CoroutineWrapper, self).__init__(**kw)
        self.coroutine = coroutine

    def process(self, baton):
        return self.coroutine.send(baton)


class RemapProcessor(base.MappingProcessor):
    """ Remaps a dictionary.

    Expects to be instantiated with a dictionary that copies values at one path to another one.

    For example, giving the mapping `{'b.c': 'a'}` and the baton
    `dict(b=dict(c='d'))`, the output will be
    `dict(a='d', b=dict(c='d'))`.
    """
    name = 'remap'
    interface.classProvides(processing.IProcessor)

    def __init__(self, extend=False, copy=False, deep_copy=False, **kw):
        super(RemapProcessor, self).__init__(**kw)
        self.extend = extend
        self.copy = copy
        self.deep_copy = deep_copy

    def process_mapping(self, input, input_path, output_path, baton, **additional_kwargs):
        output = input

        if self.deep_copy:
            output = copy.deepcopy(output)
        elif self.copy:
            output = copy.copy(output)

        if self.extend:
            output = util.dict_get_path(baton, output_path, list()) + [output]

        return output


class BatonCleaner(base.Processor):
    """ Filters the baton by removing unwanted attributes.

    Expects at least one of the two keyword arguments:

    :param keep: List of attributes to keep. Any attributes not in
        this list will be removed.

    :param remove: List of attributes to remove. Any attribute in this
        list will be removed.
    """
    name = 'clean-baton'
    interface.classProvides(processing.IProcessor)

    def __init__(self, keep=None, remove=None, **kw):
        super(BatonCleaner, self).__init__(**kw)
        self.remove = set(remove if remove else [])
        self.keep = set(keep if keep else []) - self.remove

        assert self.keep or self.remove, "Useless configuration -- nothing to remove or keep"

    def process(self, baton):
        for path in self.remove:
            util.dict_remove_path(baton, path)
        if self.keep:
            keepers = dict()
            for path in self.keep:
                value = util.dict_get_path(baton, path, Ellipsis)
                if value is Ellipsis:
                    continue

                util.dict_set_path(keepers, path, value)

            # Modify baton in-place
            baton.clear()
            baton.update(keepers)

        return baton


class BatonCollector(base.Processor):
    """ Appends batons that pass through it to the list its
    instantiated with. Useful to e.g. inspect how a baton appears at
    various stages of the processing, or as a sink.
    """
    name = 'collect-batons'
    interface.classProvides(processing.IProcessor)

    def __init__(self, list=None, deepcopy=False, describer=None, **kw):
        """
        :param deepcopy: Whether to deepcopy the batons as they pass
            through.  If enabled, this will show the batons as they
            were when they passed through --- if not, subsequent
            processors may have modified it.
        """
        super(BatonCollector, self).__init__(**kw)
        self.list = list
        if list is None:
            self.list = []
        self.deepcopy = deepcopy

        assert describer is None or callable(describer), "describer must be a callable"
        self.describer = describer

    def process(self, baton):
        if self.deepcopy:
            copy_of_baton = util.safe_deepcopy(baton)
        else:
            copy_of_baton = baton

        if self.describer:
            self.list.append(self.describer(copy_of_baton))
        else:
            self.list.append(copy_of_baton)
        return baton


class PrettyPrint(base.Processor):
    """ Prettyprints the baton before passing it on.

    No changes are made to the baton.
    """
    name = 'pretty-print'
    interface.classProvides(processing.IProcessor)

    def __init__(self, path='', formatter='baton: baton', namespace=None, *a, **kw):
        super(PrettyPrint, self).__init__(*a, **kw)

        self.path = path

        self.formatter_definition = formatter
        self.namespace = namespace or dict()

    def configure(self, runtime_environment):
        self.formatter = util.create_lambda_function(self.formatter_definition, self=self, **self.namespace)

    def process(self, baton):
        value = util.dict_get_path(baton, self.path)
        pprint.pprint(self.formatter(value))
        return baton


class PrintTraceback(base.Processor):
    """ Prints the currently active exception traceback. Useful for debugging.

    No changes are made to the baton.
    """
    name = 'print-failure-traceback'
    interface.classProvides(processing.IProcessor)

    def process(self, baton):
        f = failure.Failure()
        f.printTraceback()
        return baton

class TrapFailure(base.Processor):
    """ Traps failures of the specified types.

    If the encountered exception is not one of the expected exception types, this
    processor will raise the original exception, preserving the traceback.
    """
    name = 'trap-failure'
    interface.classProvides(processing.IProcessor)

    def __init__(self, error_types, output_path=None, *a, **kw):
        """
        :param error_types: A single or a list of fully qualified exception class
            names that should be trapped.
        :param output_path: If one of the expected error types are trapped, this
            value will be set to the matching error type.
        """
        super(TrapFailure, self).__init__(*a, **kw)

        if not isinstance(error_types, (list, tuple)):
            error_types = [error_types]

        for i, error_type in enumerate(error_types):
            error_types[i] = reflect.namedAny(error_type)

        self.error_types = error_types
        self.output_path = output_path

    def process(self, baton):
        f = failure.Failure()
        trapped = f.trap(*self.error_types)
        baton = self.get_resulting_baton(baton, self.output_path, f)
        return baton


class FlattenDictionaryList(base.InputOutputProcessor):
    """ Reduce a list of dictionaries to a list of values, given a key
    which occurs in the dictionaries.

    For example, if we have a list::
        l = [dict(author='J. Doe', email='f@o.o'), dict(author='J. Smith', id=42))]
    then processing that list given `key_path='author'` will result in::
        result = ['J. Doe', 'J. Smith']
    """

    interface.classProvides(processing.IProcessor)
    name = 'flatten-list-of-dictionaries'

    def __init__(self, key_path, uniquify=False, sort=True, **kw):
        super(FlattenDictionaryList, self).__init__(**kw)
        self.key_path = key_path
        self.uniquify = uniquify
        self.sort = sort

    def process_input(self, input, baton):
        replacements = []
        for dictionary in input:
            value = util.dict_get_path(dictionary, self.key_path)
            if value:
                replacements.append(value)
        if self.uniquify:
            replacements = list(set(replacements))
        if self.sort:
            replacements.sort()

        return replacements


class LambdaProcessor(base.InputOutputProcessor):
    """
    Given a path to a value in the baton, apply the provided lambda
    function.

    :param lambda:
        A string that defines a lambda function when `eval()`-ed. Note
        that the `lambda`-keyword should not be provided in the
        string.

    :param dependencies:
        A dict of local dependency names to their resource configurations.
        Providers that are strings are converted to tuples as required by
        the dependency manager.
        See found.processing.dependency.DependencyManager.create_dependency_map

    :param namespace:
        A dict that defines the namespace the lambda runs in. Values in
        this dict will be passed to reflect.namedAny before being made
        available to the lambda function.
    """
    interface.classProvides(processing.IProcessor)
    name = 'eval-lambda'

    def __init__(self, namespace=None, dependencies=None, **kw):
        if not 'lambda' in kw:
            raise exceptions.ConfigurationError('no lambda definition provided')
        self.lambda_definition = kw.pop('lambda')
        self.namespace = namespace or dict()

        super(LambdaProcessor, self).__init__(**kw)

        self.dependency_map = dependencies or dict()

    def configure(self, runtime_environment):
        for name, dependency_configuration in self.dependency_map.items():
            # if the configuration is a string, assume the string is a provider
            if isinstance(dependency_configuration, basestring):
                self.dependency_map[name] = dict(provider=dependency_configuration)
        
        self.runtime_environment = runtime_environment
        self.dependencies = runtime_environment.create_dependency_map(self, **self.dependency_map)
        self.lambda_ = util.create_lambda_function(self.lambda_definition, self=self, **self.namespace)

    def process_input(self, input, baton):
        try:
            return self.lambda_(input)
        except:
            log.error('Failed "%s"' % self.lambda_definition)
            raise

    def __unicode__(self):
        return u'LambdaProcessor(%s ; %s -> %s)' % (self.lambda_definition, self.input_path, self.output_path)


class ExecProcessor(base.InputOutputProcessor):
    """
    Given a path to a value in the baton, execute the provided code.

    :param code:
        A string that defines the code to run. The code will we wrapped in
        a function with the following signature: ``def compiled_process(self, input, baton):``.

    :param inline_callbacks:
        Whether to wrap ``compiled_process`` in :meth:`twisted.internet.defer.inlineCallbacks`

    :param use_file:
        If True, write the code contents to a temporary file so that the code is shown
        in any tracebacks.

    :param dependencies:
        A dict of local dependency names to their resource configurations.
        Providers that are strings are converted to tuples as required by
        the dependency manager.
        See :meth:`found.processing.dependency.DependencyManager.create_dependency_map`

    :param namespace:
        A dict that defines the namespace the code runs in. Values in
        this dict will be passed to reflect.namedAny before being made
        available to the code as part of the globals.
    """
    interface.classProvides(processing.IProcessor)
    name = 'exec-code'

    def __init__(self, code, inline_callbacks=False, use_file=True, namespace=None, dependencies=None, **kw):
        super(ExecProcessor, self).__init__(**kw)

        self.code = 'def compiled_process(self, input, baton):\n'
        self.code += self._reindent(self._trim(code))
        if inline_callbacks:
            self.code += '\n'
            self.code += 'compiled_process = defer.inlineCallbacks(compiled_process)'

        self.use_file = use_file

        self.namespace = namespace or dict()
        self.dependency_map = dependencies or dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

        # request dependencies
        if self.dependency_map:
            for dependency_key, dependency_configuration in self.dependency_map.items():
                # if the configuration is a string, assume the string is a provider
                if isinstance(dependency_configuration, basestring):
                    dependency_configuration = dict(provider=dependency_configuration)
                    self.dependency_map[dependency_key] = dependency_configuration

            self.dependencies = runtime_environment.create_dependency_map(self, **self.dependency_map)

        # configure the locals and globals the code should be executed with
        compiled_locals = dict()
        compiled_globals = dict(globals())
        for key, value in self.namespace.items():
            if isinstance(value, basestring):
                value = reflect.namedAny(value)
            compiled_globals[key] = value

        name = self.node_name or self.name

        if self.use_file:
            # if we're asked to use a file, we write the contents to a temporary
            # file and make compile() reference that file, which makes the lines
            # available in stack traces.
            self.tempfile = tempfile.NamedTemporaryFile(suffix='-'+name+'.py')
            self.tempfile.file.write(self.code)
            self.tempfile.file.flush()

            name = self.tempfile.name

        self.compiled_code = compile(self.code, filename=name, mode='exec')
            
        # compile the code and extract the compiled process function
        exec self.compiled_code in compiled_globals, compiled_locals
        self.compiled_process = compiled_locals['compiled_process']

    @defer.inlineCallbacks
    def process_input(self, input, baton):
        output = yield self.compiled_process(self, input, baton)
        defer.returnValue(output)

    # This is the example docstring processing code from http://www.python.org/dev/peps/pep-0257/
    def _trim(self, docstring):
        if not docstring:
            return ''
        # Convert tabs to spaces (following the normal Python rules)
        # and split into a list of lines:
        lines = docstring.expandtabs().splitlines()
        # Determine minimum indentation (first line doesn't count):
        indent = sys.maxint
        for line in lines[1:]:
            stripped = line.lstrip()
            if stripped:
                indent = min(indent, len(line) - len(stripped))
        # Remove indentation (first line is special):
        trimmed = [lines[0].strip()]
        if indent < sys.maxint:
            for line in lines[1:]:
                trimmed.append(line[indent:].rstrip())
        # Strip off trailing and leading blank lines:
        while trimmed and not trimmed[-1]:
            trimmed.pop()
        while trimmed and not trimmed[0]:
            trimmed.pop(0)
        # Return a single string:
        return '\n'.join(trimmed)

    def _reindent(self, string, spaces=4):
        lines = [spaces * ' ' + line for line in string.splitlines()]
        return '\n'.join(lines)


class Passthrough(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'passthrough'

    def process(self, baton):
        return baton


class LambdaConditional(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'lambda-decider'

    def __init__(self, input_path='', namespace=None, **kw):
        if not 'lambda' in kw:
            raise exceptions.ConfigurationError('no lambda definition provided')
        self.lambda_definition = kw.pop('lambda')
        self.namespace = namespace or dict()
        super(LambdaConditional, self).__init__(**kw)
        self.input_path = input_path

    def configure(self, runtime_environment):
        self.lambda_ = util.create_lambda_function(self.lambda_definition, self=self, **self.namespace)

    def process(self, baton):
        return baton

    def get_consumers(self, baton):
        input = util.dict_get_path(baton, self.input_path)

        index_or_indexes = self.lambda_(input)

        if isinstance(index_or_indexes, int):
            return [self.consumers[index_or_indexes]]
        else:
            return [self.consumers[index] for index in index_or_indexes]


class Stopper(base.Processor):
    """ Stops processing when the configured *lambda* returns true.

    An optional *input_path* can be specified, as well as a
    *namespace*. These are explained in detail in TODO: Some refererence.

    """
    interface.classProvides(processing.IProcessor)
    name = 'stop'

    def __init__(self, input_path='', decider='input: bool(input)', namespace=None, **kw):
        self.input_path = input_path
        if namespace is None:
            namespace = dict()
        self.namespace = namespace
        self.decider = util.create_lambda_function(decider, self=self, **self.namespace)
        super(Stopper, self).__init__(**kw)

    def process(self, baton):
        return baton

    def get_consumers(self, baton):
        input = util.dict_get_path(baton, self.input_path)
        should_stop = self.decider(input)
        if should_stop:
            return []
        else:
            return self.consumers


class Waiter(base.Processor):
    name = 'wait'
    interface.classProvides(processing.IProcessor)

    def __init__(self, delay, **kw):
        super(Waiter, self).__init__(**kw)
        self.delay = delay

    def process(self, baton):
        d = defer.Deferred()
        reactor.callLater(self.delay, d.callback, baton)
        return d


class NthPrinter(base.Processor):
    name = 'print-nth'
    interface.classProvides(processing.IProcessor)

    def __init__(self, n, prefix='', formatter='i, baton: i', namespace=None, **kw):
        super(NthPrinter, self).__init__(**kw)

        self.n = n
        self.prefix = prefix
        self.i = 0

        self.formatter_definition = formatter
        self.namespace = namespace or dict()

    def configure(self, runtime_environment):
        self.formatter = util.create_lambda_function(self.formatter_definition, self=self, **self.namespace)

    def process(self, baton):
        self.i += 1
        if not self.i % self.n:
            print '%s%r' % (self.prefix, self.formatter(self.i, baton))
        return baton


class DictGrouper(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'group-by-value'

    def __init__(self, key_path, input_path, output_path=None, fallback=None, **kw):
        super(DictGrouper, self).__init__(**kw)
        self.key_path = key_path
        self.input_path = input_path
        self.output_path = output_path
        if output_path is None:
            self.output_path = self.input_path
        self.fallback = fallback

    def process(self, baton):
        groups = collections.defaultdict(list)
        items = util.dict_get_path(baton, self.input_path, list())
        for item in items:
            value = util.dict_get_path(item, self.key_path, self.fallback)
            groups[value].append(item)
        util.dict_set_path(baton, self.output_path, groups)
        return baton


class NestedListFlattener(base.Processor):
    """ Flatten nested lists into a single list.

    For example:

    >>> baton = dict(data=['One', ['Two', 'Three'], ['Four']])
    >>> NestedListFlattener(paths=['data']).process(baton)
    {'data': ['One', 'Two', 'Three', 'Four']}

    """
    interface.classProvides(processing.IProcessor)
    name = 'flatten-nested-lists'

    def __init__(self, paths, path_prefix='', **kw):
        super(NestedListFlattener, self).__init__(**kw)
        self.paths = [path_prefix + path for path in paths]

    def process(self, baton):
        for path in self.paths:
            value = util.dict_get_path(baton, path, None)
            if isinstance(value, list):
                util.dict_set_path(baton, path, util.flatten(value))
        return baton


class StringEncoder(base.InputOutputProcessor):
    name = 'encode-string'
    interface.classProvides(processing.IProcessor)

    def __init__(self, encoding, **kw):
        super(StringEncoder, self).__init__(**kw)
        self.encoding = encoding

    def process_input(self, input, baton):
        return input.encode(self.encoding)


class StringDecoder(base.InputOutputProcessor):
    name = 'decode-string'
    interface.classProvides(processing.IProcessor)

    def __init__(self, encoding, **kw):
        super(StringDecoder, self).__init__(**kw)
        self.encoding = encoding

    def process_input(self, input, baton):
        return input.decode(self.encoding)


class StringFormatter(base.InputOutputProcessor):
    """ Formats a string.

    See the `format string syntax <http://docs.python.org/library/string.html#formatstrings>`_
    in the Python documentation.

    """
    name = 'format-string'
    interface.classProvides(processing.IProcessor)

    def __init__(self, format=None, format_path=None, unpack=True, **kw):
        """
        :param format: An inline format string.
        :param format_path: Path to a format string within the baton.
        :param unpack: Whether to unpack input lists, tuples and dicts when formatting. This
            enables using a variable number of arguments and keyword arguments to
            :meth:`str.format` depending on the input value.
        """
        super(StringFormatter, self).__init__(**kw)

        self.format_string = format
        self.format_string_path = format_path

        self.unpack = unpack

        self._fail_if_configuration_is_invalid()

    def _fail_if_configuration_is_invalid(self):
        if self.format_string is not None and self.format_string_path is not None:
            e_msg = 'Cannot specify both a format and a format path.'
            detail = 'Using both a format and a format path is ambiguous.'

            raise exceptions.ConfigurationError(e_msg, detail)

        if self.format_string is None and self.format_string_path is None:
            e_msg = "Must specify either 'format' or a 'format_path'"

            raise exceptions.ConfigurationError(e_msg)

    def _get_format_string(self, baton):
        if self.format_string is not None:
            return self.format_string

        return util.dict_get_path(baton, self.format_string_path)

    def process_input(self, input, baton):
        format_string = self._get_format_string(baton)
        if self.unpack:
            if isinstance(input, (list, tuple)):
                formatted = format_string.format(*input)
            elif isinstance(input, dict):
                formatted = format_string.format(**input)
            else:
                formatted = format_string.format(input)
        else:
            formatted = format_string.format(input)
        return formatted


class StringPrefixer(StringFormatter):
    """ Prefixes the string at *input_path* with the *prefix*. """
    interface.classProvides(processing.IProcessor)
    name = 'prefix-string'

    def __init__(self, prefix, **kw):
        kw['format'] = unicode(prefix) + '{0}'
        super(StringPrefixer, self).__init__(**kw)


class PDBTraceSetter(base.Processor):
    """ Calls pdb.trace() whenever a baton is processed. """
    name = 'set-pdb-trace'
    interface.classProvides(processing.IProcessor)

    def process(self, baton):
        pdb.set_trace()
        return baton


class RaiseException(base.Processor):
    """ Raise an exception of the specified *type*.

    The exception is instantiated with the optional *args* and
    *kwargs*."""
    interface.classProvides(processing.IProcessor)
    name = 'raise-exception'

    def __init__(self, type='exceptions.Exception', args=None, kwargs=None, **kw):
        super(RaiseException, self).__init__(**kw)
        self.type = reflect.namedAny(type)
        self.args = args or list()

        if not isinstance(self.args, list):
            self.args = [self.args]

        self.kwargs = kwargs or dict()

    def process(self, baton):
        raise self.type(*self.args, **self.kwargs)


class MappingSetter(base.Processor):
    """ Takes a path-to-value-mapping and sets values at the specified
    paths.

    A *path_prefix* can be specified, if all the paths in the mapping
    share a common prefix.
    """
    interface.classProvides(processing.IProcessor)
    name = 'set-values'

    def __init__(self, mapping, path_prefix='', **kw):
        super(MappingSetter, self).__init__(**kw)
        self.mapping = mapping
        self.path_prefix = path_prefix

    def process(self, baton):
        for path, value in self.mapping.items():
            path = self.path_prefix + path
            util.dict_set_path(baton, path, value)
        return baton


class ValueSetter(base.Processor):
    """ Sets the *value* at *path*. """
    interface.classProvides(processing.IProcessor)
    name = 'set-value'

    def __init__(self, path, value, **kw):
        super(ValueSetter, self).__init__(**kw)
        self.path = path
        self.value = value

    def process(self, baton):
        util.dict_set_path(baton, self.path, self.value)
        return baton


class CounterIncrementer(base.Processor):
    """ Increases the counter found at *counter_path* with *increment*. """
    interface.classProvides(processing.IProcessor)
    name = 'increment-counter'

    def __init__(self, counter_path, increment=1, **kw):
        super(CounterIncrementer, self).__init__(**kw)
        self.counter_path = counter_path
        self.increment = increment

    def process(self, baton):
        value = util.dict_get_path(baton, self.counter_path)
        util.dict_set_path(baton, self.counter_path, value + self.increment)

        return baton


class RateReporter(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'report-rate'

    def __init__(self, counter_path, delta_path='delta', format='rate: %(rate).02f', report_zero=False, **kw):
        super(RateReporter, self).__init__(**kw)
        self.counter_path = counter_path
        self.delta_path = delta_path
        self.format = format
        self.report_zero = report_zero

    def process(self, baton):
        value = util.dict_get_path(baton, self.counter_path)
        delta = util.dict_get_path(baton, self.delta_path)
        rate = value / delta

        log_string = self.format % dict(rate=rate, delta=delta, value=value)
        if(rate != 0 or self.report_zero):
            log.info(log_string)
        return baton


class Logger(base.Processor):
    """ Logs a message with the configured log-level.

    The message is either configured at *message*, or looked up in the
    baton at *message_path*.

    The message is logged with the configured *level*.

    .. seealso:: :mod:`piped.log`
    """
    interface.classProvides(processing.IProcessor)
    name = 'log'

    def __init__(self, message=None, message_path=None, level='info', **kw):
        super(Logger, self).__init__(**kw)

        if (message is None) + (message_path is None) != 1:
            raise exceptions.ConfigurationError('specify either message or message_path')

        level = level.lower()
        if level not in ('critical', 'debug', 'error', 'failure', 'info', 'warn'):
            raise ValueError('Invalid log-level: "%s"' % level)
        self.logger = getattr(log, level)

        self.message = message
        self.message_path = message_path

    def process(self, baton):
        if self.message_path:
            message = util.dict_get_path(baton, self.message_path)
        else:
            message = self.message

        if message:
            self.logger(message)

        return baton


class DependencyCaller(base.Processor):
    """ Calls a method on a dependency.

    This processor may be useful if you want to call a method on a provided dependency.
    """
    interface.classProvides(processing.IProcessor)
    name = 'call-dependency'
    NO_ARGUMENTS = object()

    def __init__(self, dependency, method='__call__', arguments=NO_ARGUMENTS,
                 unpack_arguments=False, output_path=None, *a, **kw):
        """
        :param dependency: The dependency to use.
        :param method: The name of the method to call.
        :param arguments: The arguments to call the method with. Defaults to no
            arguments.
        :param unpack_arguments: Whether to unpack arguments
        :param output_path: Where to store the output in the baton.
        """
        super(DependencyCaller, self).__init__(*a, **kw)

        if isinstance(dependency, basestring):
            dependency = dict(provider=dependency)
        self.dependency_config = dependency

        self.method_name = method
        self.arguments = arguments
        self.unpack_arguments = unpack_arguments

        self.output_path = output_path

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.dependency = dm.add_dependency(self, self.dependency_config)

    @defer.inlineCallbacks
    def process(self, baton):
        dependency = yield self.dependency.wait_for_resource()
        method_name = self.get_input(baton, self.method_name)
        method = getattr(dependency, method_name)

        arguments = self.get_input(baton, self.arguments)

        if self.arguments == self.NO_ARGUMENTS:
            result = yield method()
        else:
            if self.unpack_arguments:
                if isinstance(arguments, dict):
                    result = yield method(**arguments)
                else:
                    result = yield method(*arguments)
            else:
                result = yield method(arguments)

        baton = self.get_resulting_baton(baton, self.output_path, result)
        defer.returnValue(baton)
