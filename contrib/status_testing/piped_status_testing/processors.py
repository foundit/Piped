# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.python import reflect
from zope import interface

from piped import util, processing
from piped_status_testing import statustest
from piped.processors import base


class ReporterCreator(base.Processor):
    """ Create a reporter for statustests.

    A reporter is responsible for handling the results of test runs. The default reporter
    supports passing test results to a separate processor, gathering the test results for
    inspection after the test suites have been run and optionally printing the results to the
    console.
    """
    interface.classProvides(processing.IProcessor)
    name = 'create-statustest-reporter'

    def __init__(self, reporter='piped_status_testing.statustest.ProcessorReporter', processor=None, arguments=None, output_path='reporter', **kw):
        """

        :param reporter: The fully qualified name of the reporter class to instantiate. This
            should be a subclass of :class:`~piped_status_testing.statustest.PipelineReporter`.

        :param processor: The name of a processor that will be used. The resulting dependency
            object is passed to the reporter as the first argument. If set to ``None`` the dependency
            will also be ``None``, and no processor will be used.

        :param arguments: A class:`dict` that contains additional arguments that are passed
            to the reporter during instantiation. Arguments with the suffix ``_path`` are
            fetched from the baton at the path given by the value and the suffix is stripped
            from the key.

        :param output_path: A path to where the reporter should be stored in the baton.
        """
        super(ReporterCreator, self).__init__(**kw)

        self.processor_config = dict(provider=processor) if isinstance(processor, basestring) else processor
        self.reporter_name = reporter

        self.output_path = output_path

        self.arguments = arguments or dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        dependency_manager = runtime_environment.dependency_manager

        self.reporter_class = reflect.namedAny(self.reporter_name)

        if self.processor_config:
            self.processor_dependency = dependency_manager.add_dependency(self, self.processor_config)
        else:
            self.processor_dependency = None

    def process(self, baton):
        reporter_arguments = dict()

        for key, value in self.arguments.items():
            if key.endswith('_path'):
                key = key[:len('_path')+1]
                value = util.dict_get_path(baton, value)

            reporter_arguments[key] = value
        
        reporter = self.reporter_class(self.processor_dependency, **reporter_arguments)

        if self.output_path == '':
            return reporter

        util.dict_set_path(baton, self.output_path, reporter)
        return baton


class WaitForReporterProcessing(base.Processor):
    """ Wait for the processor that processes reporter results to finish processing.

    Since the reporter processing may be asynchronous, this processor may be used
    to wait until all the currently queued reporter processing is completed.
    """
    interface.classProvides(processing.IProcessor)
    name = 'wait-for-statustest-reporter'

    def __init__(self, reporter_path='reporter', done=False, **kw):
        """
        :param reporter_path: The path to the reporter in the baton.
        :param done: Whether to call reporter.done() after waiting for
            the reporter processing.
        """
        super(WaitForReporterProcessing, self).__init__(**kw)

        self.reporter_path = reporter_path

        self.done = done

    @defer.inlineCallbacks
    def process(self, baton):
        reporter = util.dict_get_path(baton, self.reporter_path)

        yield reporter.wait_for_result_processing()

        if self.done:
            reporter.done()

        defer.returnValue(baton)


class StatusTestProcessor(base.Processor):
    """ A processor that runs one or more status tests.

    Usage::

        class MyTestCase(StatusTestProcessor):
            interface.classProvides(processing.IProcessor)
            name = 'my-test-name'

            class TestSomething(statustest.StatusTestCase):

                def setUp(self, pipeline_dependency):
                    self.pipeline_dependency = pipeline_dependency

                @defer.inlineCallbacks
                def statustest_foobar(self):
                    pipeline = yield self.pipeline_dependency.wait_for_resource()

                    results = yield pipeline(dict(foo='foo'))

                    # perform any required asserts
                    self.assertEquals(results, [dict(...)])

            def configure(self, runtime_environment):
                # request a resource dependency to a pipeline:
                dm = runtime_environment.dependency_manager
                self.pipeline_dependency = dm.add_dependency(self.TestSomething, dict(provider='pipeline.a_pipeline'))

            def get_namespace(self, baton):
                # this will be delivered to our Tests setUp method as keyword arguments
                return dict(pipeline_dependency=self.pipeline_dependency)
    """

    def __init__(self, reporter_path='reporter', **kw):
        """
        :param reporter_path: The path to the reporter in the baton
        """
        super(StatusTestProcessor, self).__init__(**kw)

        self.reporter_path = reporter_path

    def get_namespace(self, baton):
        """ Set up a namespace. this will be delivered to the status tests
        :meth:`~piped_status_testing.statustests.StatusTestCase.setUp` method.
        """
        return dict()

    def get_suites(self, baton, loader):
        """ Returns a generator or a list of test suites. """
        for TestCase in self.get_testcases(baton):
            yield loader.loadAnything(TestCase)

    def get_testcases(self, baton):
        """ Returns a generator or a list of testcases.

        The default implementation is to return all the instance or class variables that are
        subclasses of :class:`piped_status_testing.statustest.StatusTestCase`.
        """
        for variable_name in dir(self):
            variable = getattr(self, variable_name)
            if type(variable) is type(statustest.StatusTestCase) and issubclass(variable, statustest.StatusTestCase):
                yield variable

    @defer.inlineCallbacks
    def process(self, baton):
        reporter = util.dict_get_path(baton, self.reporter_path)

        loader = statustest.StatusTestLoader(self.get_namespace(baton))

        for suite in self.get_suites(baton, loader):
            yield suite.run(reporter)

        defer.returnValue(baton)
