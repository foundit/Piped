# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import socket

import smtplib
from twisted.trial import unittest
from twisted.internet import defer, threads
from twisted.application import service
from twisted.python import filepath
from twisted.cred import checkers
from twisted.mail import smtp

from piped import processing, dependencies, util
from piped.providers import smtp_provider


class TestESMTPSenderFactory(smtp.ESMTPSenderFactory):

    def buildProtocol(self, addr):
        protocol = smtp.ESMTPSenderFactory.buildProtocol(self, addr)
        self.protocol = protocol
        return protocol


class FakeProcessor(object):

    def __init__(self):
        self.batons = list()
        self.i = 0

    def __call__(self, baton):
        self.batons.append(baton)
        self.i += 1
        return [self.i]


class SMTPServerTestBase(unittest.TestCase):
    timeout = 20

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.application = self.runtime_environment.application = service.MultiService()

    def get_free_port(self):
        sock = socket.socket()
        sock.bind(('', 0))
        free_port = sock.getsockname()[1]
        sock.close()
        return free_port

    def create_smtp_server(self, **kwargs):
        self.port = self.get_free_port()

        listen = kwargs.pop('listen', 'tcp:%i:interface=localhost')%self.port
        kwargs.setdefault('processor', 'pipeline.test_pipeline')
        kwargs.setdefault('validate_to', 'protocol, user: str(user.dest)=="bar@bar.com"')
        kwargs.setdefault('validate_from', 'protocol, helo, origin: str(origin)=="foo@foo.com"')

        self.server = smtp_provider.PipedSMTPServer(listen, **kwargs)
        self.server.setServiceParent(self.application)
        self.server.configure(self.runtime_environment)


class SMTPServerTest(SMTPServerTestBase):

    def setUp(self):
        super(SMTPServerTest, self).setUp()

        self.processor = FakeProcessor()
        self.processor_dependency = dependencies.InstanceDependency(self.processor)
        self.processor_dependency.is_ready = True

    def assertSMTPErrorCode(self, code, func, *args, **kwargs):
        exc = self.assertRaises(smtplib.SMTPResponseException, func, *args, **kwargs)
        self.assertEquals(exc.smtp_code, code)

    @defer.inlineCallbacks
    def test_simple_server(self):
        self.runtime_environment.configure()
        self.create_smtp_server()

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        def do_test():
            smtp = smtplib.SMTP('localhost', self.port, timeout=1)
            self.addCleanup(threads.deferToThread, smtp.quit)
            
            # check that validate_to and _from results in smtp errors
            self.assertRaises(smtplib.SMTPSenderRefused, smtp.sendmail, 'foo@invalid', 'bar@bar.com', 'Hello')
            self.assertRaises(smtplib.SMTPSenderRefused, smtp.sendmail, 'foo@invalid', 'bar@invalid', 'Hello')
            self.assertRaises(smtplib.SMTPRecipientsRefused, smtp.sendmail, 'foo@foo.com', 'bar@invalid', 'Hello')

            smtp.sendmail('foo@foo.com', 'bar@bar.com', 'Hello')

        yield threads.deferToThread(do_test)

        self.assertEquals(len(self.processor.batons), 1)

        baton = self.processor.batons[0]
        self.assertEquals(baton['from_addr'], 'foo@foo.com')
        self.assertEquals(baton['to_addr'], 'bar@bar.com')

        # we don't use a checker, so there should be no avatar_id in the baton:
        self.assertNotIn('avatar_id', baton)

    @defer.inlineCallbacks
    def test_custom_received_header(self):
        self.runtime_environment.configure()
        # create a custom received header:
        self.create_smtp_server(received_header='proto, helo, origin, recipients: "Received: by test_custom_header.local"')

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        yield smtp_provider.send_mail('foo@foo.com', 'bar@bar.com', 'Test message', port=self.port)

        # we should have processed 1 baton, which contains an email message with our custom received header:
        self.assertEquals(len(self.processor.batons), 1)
        baton = self.processor.batons[0]
        self.assertEquals(baton['message']['Received'], 'by test_custom_header.local')

    @defer.inlineCallbacks
    def test_authentication_required_without_tls(self):
        processor = FakeProcessor()
        processor_dependency = dependencies.InstanceDependency(processor)
        processor_dependency.is_ready = True

        self.runtime_environment.configure()
        self.create_smtp_server(checker=dict(
            name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
            arguments = dict(
                username = 'password'
            )
        ))

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        def do_test_without_tls():
            smtp = smtplib.SMTP('localhost', self.port, timeout=1)
            self.addCleanup(threads.deferToThread, smtp.quit)

            # since we configured a checker, it should be required
            self.assertSMTPErrorCode(550, smtp.sendmail, 'foo@foo.com', 'bar@bar.com', 'Hello')

            # a plaintext login is not allowed
            self.assertSMTPErrorCode(530, smtp.login, 'username', 'password')

            # tls is not configured
            self.assertRaises(smtplib.SMTPException, smtp.starttls)

        yield threads.deferToThread(do_test_without_tls)

    @defer.inlineCallbacks
    def test_authentication_required_with_tls(self):
        processor = FakeProcessor()
        processor_dependency = dependencies.InstanceDependency(processor)
        processor_dependency.is_ready = True

        self.runtime_environment.configure()

        data_dir = filepath.FilePath(__file__).sibling('data')
        self.create_smtp_server(
            tls = dict(
                private_key = data_dir.child('server.key').path,
                certificate = data_dir.child('server.crt').path
            ),
            checker = dict(
                name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
                arguments = dict(
                    username = 'password'
                )
            )
        )

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        def do_test_with_tls():
            smtp = smtplib.SMTP('localhost', self.port, timeout=1)
            self.addCleanup(threads.deferToThread, smtp.quit)

            # since we configured a checker, it should be required
            self.assertSMTPErrorCode(550, smtp.sendmail, 'foo@foo.com', 'bar@bar.com', 'Hello')

            # a plaintext login is not allowed
            self.assertSMTPErrorCode(530, smtp.login, 'username', 'password')

            # tls is configured, so we can start it
            smtp.starttls()

            # we still need to login to send an email
            self.assertSMTPErrorCode(550, smtp.sendmail, 'foo@foo.com', 'bar@bar.com', 'Hello')

            # an invalid login does not work
            self.assertSMTPErrorCode(535, smtp.login, 'invalid', 'invalid')
            self.assertSMTPErrorCode(535, smtp.login, 'username', 'invalid')
            self.assertSMTPErrorCode(535, smtp.login, 'invalid', 'password')

            # after a valid login, we can send the email:
            smtp.login('username', 'password')
            smtp.sendmail('foo@foo.com', 'bar@bar.com', 'Hello')

        yield threads.deferToThread(do_test_with_tls)

        # a mail should have arrived
        self.assertEquals(len(self.processor.batons), 1)

        baton = self.processor.batons[0]
        self.assertEquals(baton['from_addr'], 'foo@foo.com')
        self.assertEquals(baton['to_addr'], 'bar@bar.com')

        # since we logged in, the avatar_id should be in the baton:
        self.assertEquals(baton['avatar_id'], 'username')

    @defer.inlineCallbacks
    def test_simple_send_using_ssl(self):
        processor = FakeProcessor()
        processor_dependency = dependencies.InstanceDependency(processor)
        processor_dependency.is_ready = True

        self.runtime_environment.configure()

        data_dir = filepath.FilePath(__file__).sibling('data')
        private_key = data_dir.child('server.key').path
        cert_key = data_dir.child('server.crt').path

        self.create_smtp_server(
            listen = 'ssl:%i'+':interface=localhost:privateKey=%s:certKey=%s'%(private_key, cert_key),
            checker = dict(
                name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
                arguments = dict(
                    username = 'password'
                )
            )
        )

        self.application.startService()
        # give the ssl mail service two reactor iterations to stop:
        self.addCleanup(util.wait, 0)
        self.addCleanup(util.wait, 0)
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        # sending an email without authorizing should not work
        sending = smtp_provider.send_mail(
            from_addr='foo@foo.com', to_addr='bar@bar.com', file='hello',
            port=self.port, use_ssl=True,
        )
        
        try:
            yield sending
        except smtp.SMTPDeliveryError as e:
            self.assertEquals(e.code, 550)
        else:
            self.fail('SMTPDeliveryError not raised.')

        # .. but sending an email after authentication should be fine:
        yield smtp_provider.send_mail(
            from_addr='foo@foo.com', to_addr='bar@bar.com', file='hello after login',
            username='username', password='password',
            port=self.port, use_ssl=True,
        )

        # the email should have arrived
        self.assertEquals(len(self.processor.batons), 1)

        baton = self.processor.batons[0]
        self.assertEquals(baton['from_addr'], 'foo@foo.com')
        self.assertEquals(baton['to_addr'], 'bar@bar.com')

        # since we logged in, the avatar_id should be in the baton:
        self.assertEquals(baton['avatar_id'], 'username')

        # the payload should contain our text :)
        self.assertEquals(baton['message'].get_payload(), 'hello after login\n')

    
    @defer.inlineCallbacks
    def test_implicit_anonymous_authentication(self):
        processor = FakeProcessor()
        processor_dependency = dependencies.InstanceDependency(processor)
        processor_dependency.is_ready = True

        self.runtime_environment.configure()
        self.create_smtp_server(checker=dict(
            name = 'twisted.cred.checkers.AllowAnonymousAccess'
        ))

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        def do_test_without_tls():
            smtp = smtplib.SMTP('localhost', self.port, timeout=1)
            self.addCleanup(threads.deferToThread, smtp.quit)

            # a plaintext login is not allowed
            self.assertSMTPErrorCode(530, smtp.login, 'username', 'password')

            # tls is not configured
            self.assertRaises(smtplib.SMTPException, smtp.starttls)

            # we configured a checker that accepts anonymous access, so sending should work
            smtp.sendmail('foo@foo.com', 'bar@bar.com', 'Hello')

        yield threads.deferToThread(do_test_without_tls)

        self.assertEquals(len(self.processor.batons), 1)

        baton = self.processor.batons[0]
        self.assertEquals(baton['from_addr'], 'foo@foo.com')
        self.assertEquals(baton['to_addr'], 'bar@bar.com')

        # even with anonymous access the avatar_id should be set:
        self.assertEquals(baton['avatar_id'], checkers.ANONYMOUS)


class SMTPProviderTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.cm = self.runtime_environment.configuration_manager
        self.application = self.runtime_environment.application = service.MultiService()

    def test_servers_created(self):
        self.runtime_environment.configure()
        self.cm.set('smtp', dict(
            one = dict(listen='tcp:0', processor='pipeline.one'),
            two = dict(listen='tcp:0', processor='pipeline.two'),
        ))

        provider = smtp_provider.SMTPProvider()
        provider.configure(self.runtime_environment)

        self.assertEqual(len(provider.services), 2)

        requested_pipelines = list()

        for service in provider.services:
            requested_pipelines.append(service.processor_dependency.provider)

        self.assertEquals(sorted(requested_pipelines), ['pipeline.one', 'pipeline.two'])