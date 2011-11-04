# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.application import service
from twisted.internet import defer
from twisted.python import reflect
from twisted.trial import unittest
from zope import interface

from piped import exceptions, processing
from piped.providers import process_provider


class StubTestOutputProtocol(object):
    interface.implements(process_provider.IProcessOutputProtocol)

    def __init__(self, provider, type, **options):
        self.provider = provider
        self.type = type
        self.options = options
        self.data = list()

    def dataReceived(self, data):
        self.data.append(data)


class ProcessProviderTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.service = service.IService(self.runtime_environment.application)

    def tearDown(self):
        if self.service.running:
            self.service.stopService()

    def test_expected_resources_are_provided(self):
        runtime_environment = self.runtime_environment
        runtime_environment.configuration_manager.set('processes', dict(test=dict(executable='echo', spawn=False, spawn_consumerless=False)))
        self.service.startService()

        dependency_manager = runtime_environment.dependency_manager
        dependency_manager.configure(runtime_environment)

        provider = process_provider.ProcessProvider()
        provider.configure(runtime_environment)

        resource_manager = runtime_environment.resource_manager

        resource_dependency = dependency_manager.as_dependency(dict(provider='process.test'))
        resource_manager.resolve(resource_dependency)
        resource = resource_dependency.get_resource()
        self.assertIsInstance(resource, process_provider.DefaultProcessProtocol)

        for resource_type in 'stdout', 'stderr', 'stdin':
            resource_dependency = dependency_manager.as_dependency(dict(provider='process.test.%s' % resource_type))

            resource_manager.resolve(resource_dependency)
            resource = resource_dependency.get_resource()

            if resource_type in ('stdout', 'stderr'):
                self.assertIsInstance(resource, process_provider.DefaultOutput)
            else:
                self.assertIsInstance(resource, process_provider.DefaultInput)


class DefaultProcessProtocolTest(unittest.TestCase):
    timeout = 1 # if we are waiting for more than 1 second for these tests, something has gone wrong

    def _make_protocol(self, **protocol_config):
        protocol_config.setdefault('provider', None)
        protocol_config.setdefault('process_name', 'test')
        protocol_config.setdefault('executable', None)
        protocol_config.setdefault('stdout', dict(processor='FAKE'))
        protocol_config.setdefault('stderr', dict(processor='FAKE'))

        return process_provider.DefaultProcessProtocol(**protocol_config)

    def _make_baton_collector(self, output_protocol):
        return output_protocol.deferred_queue

    @defer.inlineCallbacks
    def test_simple_data_received(self):
        protocol = self._make_protocol()
        stdout = self._make_baton_collector(protocol.stdout_protocol)
        stderr = self._make_baton_collector(protocol.stderr_protocol)

        protocol.outReceived('stdout test\n')
        stdout_baton = yield stdout.get()
        self.assertEquals(stdout_baton, dict(line='stdout test'))

        protocol.errReceived('stderr test\n')
        stderr_baton = yield stderr.get()
        self.assertEquals(stderr_baton, dict(line='stderr test'))

    @defer.inlineCallbacks
    def test_newline_delimiter(self):
        protocol = self._make_protocol()
        stdout = self._make_baton_collector(protocol.stdout_protocol)

        data = 'this\nis\na test string th\nat is us\ted\r to\ntest\t the newline delimiter\n\n'
        expected_lines = ['this', 'is', 'a test string th', 'at is us\ted\r to', 'test\t the newline delimiter', '']

        protocol.outReceived(data)
        for line in expected_lines:
            stdout_baton = yield stdout.get()
            self.assertEquals(stdout_baton, dict(line=line))
        self.assertEquals(len(stdout.pending), 0, 'There should be no more batons left in stdout.')

    @defer.inlineCallbacks
    def test_custom_delimiter(self):
        protocol = self._make_protocol(stdout=dict(delimiter='e', processor='FAKE'))
        stdout = self._make_baton_collector(protocol.stdout_protocol)

        data = 'the quick brown fox jumps over the lazy dog'
        expected_lines = ['th', ' quick brown fox jumps ov', 'r th']

        protocol.outReceived(data)
        for line in expected_lines: # we ignore the last split result
            stdout_baton = yield stdout.get()
            self.assertEquals(stdout_baton, dict(line=line))
        self.assertEquals(len(stdout.pending), 0, 'There should be no more batons left in stdout.')

        # the last part of the data should still be in the buffer, adding the delimiter will create the last baton
        protocol.outReceived('e')
        stdout_baton = yield stdout.get()
        self.assertEquals(stdout_baton, dict(line=' lazy dog'))

    def test_custom_output_protocol(self):
        test_protocol_name = reflect.fullyQualifiedName(StubTestOutputProtocol)
        protocol = self._make_protocol(stdout=dict(protocol=test_protocol_name, extra_option=42))

        self.assertEqual(protocol.stdout_protocol.options, dict(extra_option=42))

        data = ['foo', 'bar', 'baz']
        for d in data:
            protocol.outReceived(d)

        self.assertEquals(protocol.stdout_protocol.data, data)

    @defer.inlineCallbacks
    def test_redirecting_stderr_to_stdout(self):
        test_protocol_name = reflect.fullyQualifiedName(process_provider.RedirectToStdout)
        protocol = self._make_protocol(stderr=dict(protocol=test_protocol_name))
        stdout = self._make_baton_collector(protocol.stdout_protocol)

        protocol.errReceived('some text\n')
        baton = yield stdout.get()
        self.assertEquals(baton, dict(line='some text'))

    def test_redirecting_stdout_raises_exception(self):
        test_protocol_name = reflect.fullyQualifiedName(process_provider.RedirectToStdout)
        self.assertRaises(exceptions.PipedError, self._make_protocol, stdout=dict(protocol=test_protocol_name))
