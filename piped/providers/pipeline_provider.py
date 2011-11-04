# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from zope import interface

from piped import exceptions, processing, resource


class PipelineProvider(object):
    """ Provides pipelines as resources.

    Pipelines are provided as resources by the ``PipelineProvider``. The pipelines are expected
    to be defined under the ``pipelines`` configuration key::

        pipelines:
            my_pipeline:
                # pipeline definition
            another:
                nested_pipeline:
                    # pipeline definition


    The above pipelines would be made available as ``pipeline.my_pipeline`` and
    ``pipeline.another.nested-pipeline``.
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        self.pipeline_by_name = dict()
        self.processor_graph_factory = processing.ProcessorGraphFactory()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = self.runtime_environment.dependency_manager

        self.processor_graph_factory.configure(runtime_environment)

        # Tell the resource_manager about the pipelines we can provide.
        resource_manager = runtime_environment.resource_manager

        for pipeline_name in self.processor_graph_factory.pipelines_configuration:
            resource_manager.register('pipeline.%s' % pipeline_name, provider=self)

        resource_manager.register('pipeline_provider', provider=self)

    def __getitem__(self, item):
        # TODO: This should dissapear. Tests currently depend on it, though.
        if not item in self.pipeline_by_name:
            e_msg = 'no such pipeline: ' + item
            detail = 'Available pipelines: ' + (', '.join(sorted(self.pipeline_by_name)))
            raise exceptions.InvalidPipelineError(e_msg, detail)
        return self.pipeline_by_name[item]

    def _get_or_create_pipeline(self, pipeline_name):
        if not pipeline_name in self.pipeline_by_name:
            pipeline = self.processor_graph_factory.make_evaluator(pipeline_name)

            # Configure the processors and give them a chance to request their own dependencies.
            pipeline.configure_processors(self.runtime_environment)

            # Make the pipeline depend on all its processors. This enables
            # the processor's to depend on resources as well.
            for i, processor in enumerate(pipeline):
                self.dependency_manager.add_dependency(pipeline, processor)

            self.pipeline_by_name[pipeline_name] = pipeline

        return self.pipeline_by_name[pipeline_name]

    def add_consumer(self, resource_dependency):
        if resource_dependency.provider == 'pipeline_provider':
            resource_dependency.on_resource_ready(self)
            return

        pipeline_name = resource_dependency.provider.split('.', 1)[1]

        # Consumers request the pipeline with a provider string. What
        # we do here is essentially making that a proxy for the
        # pipeline-instance.

        pipeline = self._get_or_create_pipeline(pipeline_name)

        pipeline_dependency = self.dependency_manager.as_dependency(pipeline)

        # Give the resource_dependency the actual pipeline resource
        resource = pipeline

        pipeline_dependency.on_ready += lambda dependency: resource_dependency.on_resource_ready(resource)
        pipeline_dependency.on_lost += lambda dependency, reason: resource_dependency.on_resource_lost(reason)

        # this isn't strictly required, but makes for an easier graph to follow
        self.dependency_manager.add_dependency(resource_dependency, pipeline_dependency)

        # if the pipeline already is ready, the on_ready will not be called later, and we give
        # the resource dependency the pipeline immediately
        if pipeline_dependency.is_ready:
            resource_dependency.on_resource_ready(resource)
