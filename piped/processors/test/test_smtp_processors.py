# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import email
from StringIO import StringIO

from twisted.trial import unittest
from twisted.internet import defer

from piped import exceptions, processing, dependencies
from piped.processors import smtp_processors
from piped.providers.test import test_smtp_provider


class FakePipeline(object):

    def __init__(self):
        self.batons = defer.DeferredQueue()
        self.i = 0

    def __call__(self, baton):
        self.batons.put(baton)
        self.i += 1
        return [self.i]


class SendEmailTest(test_smtp_provider.SMTPServerTestBase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

        self.application = self.runtime_environment.application

        self.pipeline = FakePipeline()
        self.pipeline_dependency = dependencies.InstanceDependency(self.pipeline)
        self.pipeline_dependency.is_ready = True

    @defer.inlineCallbacks
    def test_sending_an_email(self):
        self.create_smtp_server()
        self.server.processor_dependency = self.pipeline_dependency

        self.server.startService()
        self.addCleanup(self.server.stopService)

        processor = smtp_processors.SendEmail(from_path='f', to_path='t', configuration=dict(port=self.port))
        baton = dict(message='test', f='foo@foo.com', t='bar@bar.com')
        yield processor.process(baton)

        baton = yield self.pipeline.batons.get()
        self.assertEquals(baton['message'].get_payload(), 'test\n')

    @defer.inlineCallbacks
    def test_valid_from_to_formats(self):
        self.create_smtp_server()
        self.server.processor_dependency = self.pipeline_dependency

        self.server.startService()
        self.addCleanup(self.server.stopService)

        processors = list()

        processors.append(smtp_processors.SendEmail(from_path='f', to_path='t', configuration=dict(port=self.port)))
        processors.append(smtp_processors.SendEmail(from_path='f', configuration=dict(port=self.port), **{'to':'bar@bar.com'}))
        processors.append(smtp_processors.SendEmail(to_path='t', configuration=dict(port=self.port), **{'from':'foo@foo.com',}))
        processors.append(smtp_processors.SendEmail(configuration=dict(port=self.port), **{'from':'foo@foo.com', 'to':'bar@bar.com'}))

        baton = dict(message='test', f='foo@foo.com', t='bar@bar.com')
        for processor in processors:
            processor.process(baton)

        for processor in processors:
            baton = yield self.pipeline.batons.get()
            self.assertEquals(baton['from_addr'], 'foo@foo.com')
            self.assertEquals(baton['to_addr'], 'bar@bar.com')

    def test_invalid_to_from_formats(self):
        # missing to and from
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail)

        # missing either to or from:
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail, from_path='f')
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail, to_path='t')
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail, **{'from':'foo@foo.com',})
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail, **{'to':'bar@bar.com'})

        # multiple tos or froms
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail, from_path='f', to_path='t', **{'from':'foo@foo.com',})
        self.assertRaises(exceptions.ConfigurationError, smtp_processors.SendEmail, from_path='f', to_path='t', **{'to':'bar@bar.com'})

        # multiple tos and froms
        self.assertRaises(
            exceptions.ConfigurationError,
            smtp_processors.SendEmail, from_path='f', to_path='t', **{'from':'foo@foo.com', 'to':'bar@bar.com'}
        )


class TestSetMessageHeaders(unittest.TestCase):

    @defer.inlineCallbacks
    def test_adding_header(self):
        processor = smtp_processors.SetMessageHeaders(headers=dict(Subject='test_subject'))

        baton = dict(message=email.message_from_string('test_payload'))
        yield processor.process(baton)

        self.assertEquals(baton['message']['Subject'], 'test_subject')
        self.assertEquals(baton['message'].get_payload(), 'test_payload')

    @defer.inlineCallbacks
    def test_replacing_header(self):
        processor = smtp_processors.SetMessageHeaders(headers=dict(Subject='test_subject'))

        baton = dict(message=email.message_from_string('Subject: hi there\ntest_payload'))
        self.assertEquals(baton['message']['Subject'], 'hi there')
        self.assertEquals(baton['message'].get_payload(), 'test_payload')

        yield processor.process(baton)

        self.assertEquals(baton['message']['Subject'], 'test_subject')
        self.assertEquals(baton['message'].get_payload(), 'test_payload')


class TestCreateEmailMessage(unittest.TestCase):

    @defer.inlineCallbacks
    def test_creating_simple(self):
        processor = smtp_processors.CreateEmailMessage()

        baton = yield processor.process(dict())

        self.assertEquals(baton['message'].as_string(), '\n')

    @defer.inlineCallbacks
    def test_creating_simple_replacing_baton(self):
        processor = smtp_processors.CreateEmailMessage(output_path='')

        result = yield processor.process(dict())

        self.assertEquals(result.as_string(), '\n')

    @defer.inlineCallbacks
    def test_creating_with_headers(self):
        processor = smtp_processors.CreateEmailMessage(headers=dict(foo='bar'))

        baton = yield processor.process(dict())

        self.assertEquals(baton['message'].as_string(), 'foo: bar\n\n')

    @defer.inlineCallbacks
    def test_creating_with_payload(self):
        processor = smtp_processors.CreateEmailMessage(payload_path='payload')

        baton = yield processor.process(dict(payload='this is my message'))

        self.assertEquals(baton['message'].as_string(), '\nthis is my message')

    @defer.inlineCallbacks
    def test_creating_with_stream_payload(self):
        processor = smtp_processors.CreateEmailMessage(payload_path='payload')

        baton = yield processor.process(dict(payload=StringIO('this is my message')))

        self.assertEquals(baton['message'].as_string(), '\nthis is my message')