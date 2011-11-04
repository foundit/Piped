# -*- test-case-name: piped.processing.test.test_pipeline_processors -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Processors that work with pipelines --- i.e. invoking or diagramming them. """
from twisted.internet import defer
from twisted.python import failure
from zope import interface

from piped import util, processing, exceptions, log
from piped.processors import base


# TODO: Refactor to be based on MappingProcessor
class ScatterGatherer(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'scatter-gather'

    def __init__(self, mapping=None, copy=True, input_path_prefix='', output_path_prefix='', **kw):
        super(ScatterGatherer, self).__init__(**kw)
        self.mapping = mapping
        self.copy = copy
        self.input_path_prefix = input_path_prefix
        self.output_path_prefix = output_path_prefix

    def configure(self, runtime_environment):
        super(ScatterGatherer, self).configure(runtime_environment)
        self.runtime_environment = runtime_environment
        self.dependencies = self._require_dependencies()

    def get_pipeline(self, pipeline_name):
        return self.dependencies['pipeline.%s' % pipeline_name]

    def _require_dependencies(self):
        dependency_mapping = dict()
        for pipeline_name in self._get_names_of_required_pipelines():
            resource_path = 'pipeline.%s' % pipeline_name
            dependency_mapping[resource_path] = dict(provider=resource_path)

        return self.runtime_environment.create_dependency_map(self, **dependency_mapping)

    def _get_names_of_required_pipelines(self):
        return set(mapped_attribute['pipeline'] for mapped_attribute in self.mapping)

    @defer.inlineCallbacks
    def process(self, baton):
        # For every specified attribute path, check if there's
        # anything processable. If there is, process it and store the
        # result in the supplied attribute.

        ds = []
        for mapped_attribute in self.mapping:
            input_path = self.input_path_prefix + mapped_attribute.get('input_path', '')
            input_baton = util.dict_get_path(baton, input_path, Ellipsis)
            if input_baton is Ellipsis:
                continue

            pipeline_name = mapped_attribute['pipeline']
            output_path = self.output_path_prefix + mapped_attribute.get('output_path', pipeline_name)
            pipeline = self.get_pipeline(pipeline_name)

            input_baton = self.preprocess_baton(self._maybe_copy(input_baton))

            d = defer.maybeDeferred(pipeline, input_baton)
            d.addCallback(lambda _, resulting_baton=input_baton, output_path=output_path: util.dict_set_path(baton, output_path, resulting_baton))

            ds.append(d)

        results = yield defer.DeferredList(ds)
        for success, result in results:
            if not success:
                # It's a failure. Reraise it, so the resulting
                # stack-trace is correct. Note that if there are
                # failures in multiple pipelines, only the first one
                # is raised.
                result.raiseException()

        defer.returnValue(baton)

    def _maybe_copy(self, baton):
        if self.copy:
            return baton.copy()
        return baton

    def preprocess_baton(self, baton):
        return baton


class PipelineRunner(base.InputOutputProcessor):
    """" Processes a baton in another pipeline.

    If a ``pipeline_path`` is specified, the processor creates a temporary dependency during
    processing and takes care of removing this dependency afterwards. If this processor is the
    first consumer of the pipeline, exceptions that would otherwise be raised during startup may
    be raised during this processors processing.

    Note that the pipeline is only requested as a dependency, and not deconstructed after use. This
    means that unused resources may remain in process afterwards.
    """
    interface.classProvides(processing.IProcessor)
    name = 'run-pipeline'

    def __init__(self, pipeline=Ellipsis, pipeline_path=Ellipsis, only_last_result=True, trace_path=None, *a, **kw):
        """
        :param pipeline: The name of the pipeline to process the baton in. A name
            may either be absolute or relative. Relative names start with a ``.``,
            and each dot means that the named pipeline is one level higher up. To
            reference a sibling pipeline, start with a single dot.
        :param pipeline_path: The path to where the pipeline name in the baton. The pipeline
            name in the baton is resolved in the same way as the ``pipeline`` argument.
            Using both a ``pipeline`` and ``pipeline_path`` argument to this processor results
            in a configuration error.
        :param only_last_result: Whether to use only the last result. Since
            a pipeline may have several sinks, the results from processing will always
            be a `list`, and this option discards any output from any baton except
            the last.
        :param trace_path: Path to store tracing results to. Cannot be an empty string.
            Defaults to ``None``, which means no tracing.
        """
        super(PipelineRunner, self).__init__(*a, **kw)

        self.pipeline_name = pipeline
        self.pipeline_path = pipeline_path
        self.only_last_result = only_last_result

        self.trace_path = trace_path

        self._fail_if_trace_path_is_invalid(trace_path)
        self._fail_if_pipeline_is_invalid()

        if self.pipeline_path is not Ellipsis:
            self._process_input = self.process_dynamic_input
        else:
            self._process_input = self.process_static_input

    def _fail_if_trace_path_is_invalid(self, trace_path):
        if trace_path == '':
            e_msg = 'Invalid trace path: %r' % trace_path
            detail = ('The trace path cannot be the empty string, since replacing the baton with the '
                      'trace results are not supported at this time.')

            raise exceptions.ConfigurationError(e_msg, detail)

    def _fail_if_pipeline_is_invalid(self):
        if (self.pipeline_name is Ellipsis) + (self.pipeline_path is Ellipsis) != 1:
            e_msg = 'Invalid processor configuration.'
            detail = 'Either "pipeline" or "pipeline_path" must be specified, not both.'

            raise exceptions.ConfigurationError(e_msg, detail)

    def configure(self, runtime_environment):
        self.dependency_manager = runtime_environment.dependency_manager

        if self.pipeline_path is not Ellipsis:
            return

        # the pipeline_name may be sibling notation, so resolve it to a fully qualified pipeline name
        pipeline_name = util.resolve_sibling_import(self.pipeline_name, self.evaluator.name)

        self.pipeline_dependency = self.dependency_manager.as_dependency(dict(provider='pipeline.%s' % pipeline_name))
        self.dependency_manager.add_dependency(self, self.pipeline_dependency)

    @defer.inlineCallbacks
    def process_input(self, input, baton):
        results = yield self._process_input(input, baton)
        defer.returnValue(results)

    @defer.inlineCallbacks
    def process_static_input(self, input, baton):
        pipeline = yield self.pipeline_dependency.wait_for_resource()

        results = yield self.process_input_using_pipeline(input, baton, pipeline)

        defer.returnValue(results)

    @defer.inlineCallbacks
    def process_dynamic_input(self, input, baton):
        pipeline_name = util.dict_get_path(baton, self.pipeline_path)
        # the pipeline_name may be sibling notation, so resolve it to a fully qualified pipeline name
        pipeline_name = util.resolve_sibling_import(pipeline_name, self.evaluator.name)

        dependency = self.dependency_manager.add_dependency(self, dict(provider='pipeline.%s'%pipeline_name))

        try:
            self.dependency_manager.resolve_initial_states()
            pipeline = yield dependency.wait_for_resource()

            results = yield self.process_input_using_pipeline(input, baton, pipeline)

            defer.returnValue(results)

        finally:
            # while this is sufficient to remove the dependency between ourselves
            # and the target pipeline, it does not currently deconstruct the pipeline
            # and its resources even if it is no longer needed.
            self.dependency_manager.remove_dependency(self, dependency)

    @defer.inlineCallbacks
    def process_input_using_pipeline(self, input, baton, pipeline):
        if self.trace_path:
            results, trace = yield pipeline.traced_process(input)
            util.dict_set_path(baton, self.trace_path, trace)
        else:
            results = yield pipeline(input)

        if hasattr(results, '__getitem__') and self.only_last_result:
            results = results[-1]

        defer.returnValue(results)

    @property
    def node_name(self):
        # Escape the \, as we want Dot to see "\n"
        return 'run-pipeline:\\n%s' % self.pipeline_name


class ConditionalPipelineRunner(PipelineRunner):
    """" Processes a baton in another pipeline, if the conditional is true. """
    interface.classProvides(processing.IProcessor)
    name = 'run-pipeline-if'

    def __init__(self, condition='input: input', condition_input_path='', namespace=None, dependencies=None, **kw):
        super(ConditionalPipelineRunner, self).__init__(**kw)
        self.lambda_definition = condition
        self.condition_input_path = condition_input_path
        self.namespace = namespace or dict()
        self.dependency_map = dependencies or dict()

    def configure(self, runtime_environment):
        super(ConditionalPipelineRunner, self).configure(runtime_environment)

        for name, dependency_configuration in self.dependency_map.items():
            # if the configuration is a string, assume the string is a provider
            if isinstance(dependency_configuration, basestring):
                self.dependency_map[name] = dict(provider=dependency_configuration)

        self.runtime_environment = runtime_environment
        self.dependencies = runtime_environment.create_dependency_map(self, **self.dependency_map)
        self.lambda_ = util.create_lambda_function(self.lambda_definition, self=self, **self.namespace)

    def process(self, baton):
        input_for_conditional = util.dict_get_path(baton, self.condition_input_path)
        should_invoke_pipeline = self.lambda_(input_for_conditional)

        if not should_invoke_pipeline:
            return baton

        return super(ConditionalPipelineRunner, self).process(baton)


class PipelineDiagrammer(base.Processor):
    """ Makes a dot-representation of the pipelines of every processor
    graph-evaluator.
    """
    interface.classProvides(processing.IProcessor)
    name = 'diagram-pipelines'

    def __init__(self, output_path='dot', **kw):
        super(PipelineDiagrammer, self).__init__(**kw)
        self.output_path = output_path

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.pipeline_provider_dependency = dm.add_dependency(self, dict(provider='pipeline_provider'))

    @defer.inlineCallbacks
    def process(self, baton):
        pipeline_provider = yield self.pipeline_provider_dependency.wait_for_resource()

        subgraphs = []
        for pipeline_name, pipeline in pipeline_provider.pipeline_by_name.items():
            dot = pipeline.processor_graph.get_dot()
            dot = dot.replace('digraph G {', 'subgraph "cluster%s" { label="%s"; ' % (pipeline_name, pipeline_name))
            subgraphs.append(dot)

        dot = u'digraph G {\n%s\n}' % '\n'.join(subgraphs)
        util.dict_set_path(baton, self.output_path, dot)
        defer.returnValue(baton)


class DependencyDiagrammer(base.Processor):
    """ Makes a dot-representation of the dependency graph. """
    interface.classProvides(processing.IProcessor)
    name = 'diagram-dependencies'

    def __init__(self, output_path='dot', **kw):
        super(DependencyDiagrammer, self).__init__(**kw)
        self.output_path = output_path

    def configure(self, runtime_environment):
        self.dependency_manager = runtime_environment.dependency_manager

    def process(self, baton):
        dot = self.dependency_manager.get_dot()
        util.dict_set_path(baton, self.output_path, dot)
        return baton


class ForEach(base.InputOutputProcessor):
    """ ForEach is an In/Out-processor that invokes a pipeline for
    every item in its input.

    If the input is a dict, the processor will iterate over the values and
    the output will also be a dict where the values are the results from the
    processing.
    """
    interface.classProvides(processing.IProcessor)
    name = 'for-each'

    def __init__(self, pipeline, chunk_size=None,
                 namespace=None, result_processor='results: results[-1]',
                 parallel=False, done_on_first=False, fail_on_error=False,
                 **kw):
        """
        :param pipeline: The pipeline to process the items in.
        :param chunk_size: If specified, the input-iterable is chunked. E.g
            if `chunk_size=2`, and the input iterable is '[1, 2, 3, 4, 5]`,
            then the pipeline is invoked with three times, with the batons
            being `[1, 2]`, `[3, 4]` and `[5]`.
        :param result_processor: A lambda-definition. The lambda is invoked once for
            each item in the input with the results from the target pipeline.
        :param namespace: A dict specifiying a namespace for the result_processor.
        :param input_path: Path to the input in the baton. The input is assumed
            to be an iterable.
        :param parallel: If true, then the iterable is exhausted, and the
            pipeline is invoked with all items in parallel. Then we wait until
            all of them have completed.
        :param done_on_first: If true, then the result of the pipeline that
            completes first is returned. The results/failures of the other
            pipelines are dropped, unless they all fail. In that case, an
            `exceptions.AllPipelinesFailedError` is raised.
        :param fail_on_error: If true, then any failure in the processing
            will cause the result to be that failure. If it is false, which is
            the default, then the errors are represented as failure-instances
            in the output.
        """
        super(ForEach, self).__init__(**kw)
        self.pipeline_name = pipeline
        self.chunk_size = chunk_size
        self.namespace = namespace or dict()
        self.result_processor_definition = result_processor
        self.fail_on_error = fail_on_error
        self.done_on_first = done_on_first

        self.parallel = parallel
        if parallel:
            self._process_iterable = self._process_in_parallel
        else:
            self._process_iterable = self._process_serially

    def configure(self, runtime_environment):
        self.pipeline_dependency = runtime_environment.dependency_manager.add_dependency(self, dict(provider='pipeline.%s' % self.pipeline_name))
        self.result_processor = util.create_lambda_function(self.result_processor_definition, self=self, **self.namespace)

    @defer.inlineCallbacks
    def process_input(self, input, baton):
        if self.chunk_size:
            input = util.chunked(input, self.chunk_size)

        if isinstance(input, dict):
            keys = input.keys()
            values = input.values()
            new_values = yield self._process_iterable(values)
            result = dict(zip(keys, new_values))
        else:
            result = yield self._process_iterable(input)

        defer.returnValue(result)

    @defer.inlineCallbacks
    def _process_serially(self, input):
        results = list()
        pipeline = yield self.pipeline_dependency.wait_for_resource()

        for sub_baton in input:

            try:
                result = yield pipeline(sub_baton)
                results.append(self.result_processor(result))
            except Exception:
                if self.fail_on_error:
                    raise
                else:
                    results.append(failure.Failure())

            else:
                if self.done_on_first:
                    # This actually raises an exception, so it must be outside the above try.
                    defer.returnValue(results[-1])

        if self.done_on_first:
            # If anything would have succeeded, we would have broken
            # out above. Thus, we can assume it's all failures.
            raise exceptions.AllPipelinesFailedError('All pipelines failed', failures=results)

        defer.returnValue(results)

    @defer.inlineCallbacks
    def _process_in_parallel(self, input):
        pipeline = yield self.pipeline_dependency.wait_for_resource()
        ds = list()

        # Prepare running all pipelines in parallel.
        for sub_baton in input:
            d = defer.maybeDeferred(pipeline, sub_baton)
            # ... the result must be processed as usual.
            d.addCallback(lambda result: self.result_processor(result))
            ds.append(d)

        # Then wait.
        try:
            result = yield defer.DeferredList(ds, fireOnOneCallback=self.done_on_first,
                                              fireOnOneErrback=self.fail_on_error, consumeErrors=True)
        except defer.FirstError, e:
            e.subFailure.raiseException()

        # Unpack the results.
        if isinstance(result, list):
            result = [value for _, value in result]

            if result and all(isinstance(maybe_failure, failure.Failure) for maybe_failure in result):
                raise exceptions.AllPipelinesFailedError(e_msg='All pipelines failed', failures=result)

        elif self.done_on_first:
            result = result[0]

        defer.returnValue(result)