# -*- test-case-name: piped.processing.test.test_utilprocessors -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Processors that work with pipelines --- i.e. invoking or diagramming them. """
from twisted.internet import defer
from zope import interface

from piped import util, processing
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

            d = defer.maybeDeferred(pipeline.process, input_baton)
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
    """" Processes a baton in another pipeline. """
    interface.classProvides(processing.IProcessor)
    name = 'run-pipeline'

    def __init__(self, pipeline, only_last_result=True, *a, **kw):
        """
        :param pipeline: The name of the pipeline to process the baton in. A name
            may either be absolute or relative. Relative names start with a ``.``,
            and each dot means that the named pipeline is one level higher up. To
            reference a sibling pipeline, start with a single dot.
        :param only_last_result: Whether to use only the last result. Since
            a pipeline may have several sinks, the results from processing will always
            be a `list`, and this option discards any output from any baton except
            the last.
        """
        super(PipelineRunner, self).__init__(*a, **kw)

        self.pipeline_name = pipeline
        self.only_last_result = only_last_result

    def configure(self, runtime_environment):
        dependency_manager = runtime_environment.dependency_manager

        # the pipeline_name may be sibling notation, so resolve it to a fully qualified pipeline name
        pipeline_name = util.resolve_sibling_import(self.pipeline_name, self.evaluator.name)

        self.pipeline_dependency = dependency_manager.as_dependency(dict(provider='pipeline.%s' % pipeline_name))
        dependency_manager.add_dependency(self, self.pipeline_dependency)

    @defer.inlineCallbacks
    def process_input(self, input, baton):
        pipeline = yield self.pipeline_dependency.wait_for_resource()

        results = yield pipeline.process(input)
        if self.only_last_result:
            results = results[-1]

        defer.returnValue(results)

    @property
    def node_name(self):
        # Escape the \, as we want Dot to see "\n"
        return 'run-pipeline:\\n' + self.pipeline_name


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