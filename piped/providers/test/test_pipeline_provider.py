# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.trial import unittest

from piped import processing
from piped.providers import pipeline_provider


class PipelineProviderTest(unittest.TestCase):
    
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()
        self.configuration_manager = self.runtime_environment.configuration_manager
        self.dependency_manager = self.runtime_environment.dependency_manager

    def test_provided_resources(self):
        pp = pipeline_provider.PipelineProvider()
        self.configuration_manager.set('pipelines.test_pipeline', ['passthrough'])
        pp.configure(self.runtime_environment)

        pipeline_provider_dependency = self.dependency_manager.add_dependency(self, dict(provider='pipeline_provider'))
        pipeline_dependency = self.dependency_manager.add_dependency(self, dict(provider='pipeline.test_pipeline'))

        self.dependency_manager.resolve_initial_states()

        self.assertEquals(pipeline_provider_dependency.get_resource(), pp)

        processor = pipeline_dependency.get_resource()
        self.assertTrue(hasattr(processor, '__call__'))
        self.assertEquals(processor.__call__.im_func.func_name, 'process')
        self.assertIsInstance(processor, processing.TwistedProcessorGraphEvaluator)
