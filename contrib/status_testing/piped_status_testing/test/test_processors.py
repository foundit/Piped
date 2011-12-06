# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from StringIO import StringIO

from twisted.internet import defer
from twisted.trial import unittest

from piped import processing
from piped_status_testing import processors, statustest


class TestProcessor(processors.StatusTestProcessor):
    name = 'test-processor'
    class Test(statustest.StatusTestCase):

        def statustest_success(self):
            pass

        def statustest_failed(self):
            self.assertFalse(True)

        def statustest_error(self):
            raise Exception()

        def statustest_skip(self):
            pass
        statustest_skip.skip = 'Skip this'

        def statustest_todo(self):
            raise Exception()
        statustest_todo.todo = 'This should raise an Exception.'

        def statustest_todo_success(self):
            pass
        statustest_todo_success.todo = 'This should pass "unexpectedly".'


class TestProcessorSuccess(processors.StatusTestProcessor):
    name = 'test-processor-success'
    class Test(statustest.StatusTestCase):

        def setUp(self, processor):
            self.processor = processor

        def statustest_success(self):
            self.assertIsInstance(self.processor, TestProcessorSuccess)

    def get_namespace(self, baton):
        return dict(processor=self)


class TestSimpleStatus(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.package_name = __name__.rsplit('.', 1)[0]

    @defer.inlineCallbacks
    def test_that_test_results_are_being_processed(self):
        self.runtime_environment.configuration_manager.set('pipelines', dict(
            collect=['collect-batons'],
            status=[
                    {'call-named-any': dict(name='StringIO.StringIO', output_path='stream')},
                    {'create-statustest-reporter': dict(arguments=dict(stream_path='stream'), processor='pipeline.collect')},
                    'test-processor',
                    'test-processor-success',
                    'wait-for-statustest-reporter'
            ]
        ))

        self.runtime_environment.configure()

        pgf = processing.ProcessorGraphFactory()
        pgf.configure(self.runtime_environment)

        # register our test processor before making the pipeline
        pgf.plugin_manager._register_plugin(TestProcessor)
        pgf.plugin_manager._register_plugin(TestProcessorSuccess)

        evaluators = pgf.make_all_pipelines()
        for evaluator in evaluators.values():
            evaluator.configure_processors(self.runtime_environment)

        # get the report creator processor
        report_creator = list(evaluators['status'])[1]
        report_creator.processor_dependency.on_resource_ready(evaluators['collect'].process)
        report_creator.processor_dependency.fire_on_ready()

        processed = yield evaluators['status'].process(dict())

        # the collector processor should have collected all the test results
        collector = list(evaluators['collect'])[-1]
        self.assertEquals(len(collector.list), 7)

        # .. and the reporter should contain the test results
        reporter = processed[0]['reporter']
        self.assertEquals(reporter.testsRun, 7)
        self.assertEquals(reporter.successes, 2)
        self.assertEquals(len(reporter.failures), 1)
        self.assertEquals(len(reporter.errors), 1)
        self.assertEquals(len(reporter.skips), 1)
        self.assertEquals(len(reporter.expectedFailures), 1)
        self.assertEquals(len(reporter.unexpectedSuccesses), 1)

    @defer.inlineCallbacks
    def test_without_reporter_processor(self):
        self.runtime_environment.configuration_manager.set('pipelines', dict(
            status=[
                    {'call-named-any': dict(name='StringIO.StringIO', output_path='stream')},
                    {'create-statustest-reporter': dict(arguments=dict(stream_path='stream'))},
                    'test-processor',
                    'test-processor-success',
                    {'wait-for-statustest-reporter': dict(done=True)}
            ]
        ))

        self.runtime_environment.configure()

        pgf = processing.ProcessorGraphFactory()
        pgf.configure(self.runtime_environment)

        # register our test processor before making the pipeline
        pgf.plugin_manager._register_plugin(TestProcessor)
        pgf.plugin_manager._register_plugin(TestProcessorSuccess)

        evaluators = pgf.make_all_pipelines()
        for evaluator in evaluators.values():
            evaluator.configure_processors(self.runtime_environment)

        processed = yield evaluators['status'].process(dict())

        # the reporter should contain the test results
        reporter = processed[0]['reporter']
        self.assertEquals(reporter.testsRun, 7)
        self.assertEquals(reporter.successes, 2)
        self.assertEquals(len(reporter.failures), 1)
        self.assertEquals(len(reporter.errors), 1)
        self.assertEquals(len(reporter.skips), 1)
        self.assertEquals(len(reporter.expectedFailures), 1)
        self.assertEquals(len(reporter.unexpectedSuccesses), 1)