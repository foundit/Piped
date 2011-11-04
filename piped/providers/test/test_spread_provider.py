# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import socket

import mock
from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.application import service
from twisted.python import reflect, log
from twisted.spread import pb, flavors
from twisted.cred import credentials, error

from piped import processing, dependencies, util, exceptions
from piped.providers import spread_provider, pipeline_provider


class FakeProcessor(object):

    def __init__(self):
        self.batons = defer.DeferredQueue()
        self.i = 0

    def __call__(self, baton):
        self.batons.put(baton)
        self.i += 1
        return [self.i]


class PBTestBase(unittest.TestCase):
    timeout = 2

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.application = self.runtime_environment.application = service.MultiService()

    def replace_twisted_log_err(self, err=lambda failure=None: None):
        original = log.err
        log.err = err

        def restore():
            log.err = original
        self.addCleanup(restore)

    def get_free_port(self):
        sock = socket.socket()
        sock.bind(('', 0))
        free_port = sock.getsockname()[1]
        sock.close()
        return free_port

    def create_pb_server(self, **kwargs):
        self.port = self.get_free_port()

        listen = 'tcp:%i:interface=localhost'%self.port
        kwargs['listen'] = listen
        kwargs.setdefault('processor', 'pipeline.test_pipeline')
        kwargs.setdefault('wait_for_processor', False)

        self.server = spread_provider.PipedPBService(**kwargs)
        self.server.setServiceParent(self.application)
        self.server.configure(self.runtime_environment)


class PBServerTest(PBTestBase):

    def setUp(self):
        super(PBServerTest, self).setUp()

        self.processor = FakeProcessor()
        self.processor_dependency = dependencies.InstanceDependency(self.processor)
        self.processor_dependency.is_ready = True

        # replace twisted.python.log.err in these tests since pb uses log.err to
        # print failure messages on the server side if the exception does not subclass
        # pb.Error.
        self.replace_twisted_log_err()

        self.runtime_environment.configure()

    @defer.inlineCallbacks
    def test_simple_server(self):
        self.create_pb_server()

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        # connect to the server
        client = pb.PBClientFactory()
        reactor.connectTCP('localhost', self.port, client)

        root = yield client.getRootObject()
        self.addCleanup(root.broker.transport.loseConnection)

        # call a remote function
        adding = root.callRemote('add', 42, b=93)

        # assert that the baton is on the expected form
        baton = yield self.processor.batons.get()
        self.assertEquals(baton['message'], 'add')
        self.assertEquals(baton['args'], (42,))
        self.assertEquals(baton['kwargs'], dict(b=93))

        # callback the deferred in the baton
        baton['deferred'].callback(42+93)

        # the above callbacking should result in the client getting its response
        result = yield adding
        self.assertEquals(result, 42+93)

    @defer.inlineCallbacks
    def test_waiting_for(self):
        processor_dependency = dependencies.ResourceDependency(provider='pipeline.any')

        self.create_pb_server()

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = processor_dependency

        # connect to the server
        client = pb.PBClientFactory()

        reactor.connectTCP('localhost', self.port, client)

        root = yield client.getRootObject()
        self.addCleanup(root.broker.transport.loseConnection)

        # call a remote function without waiting for the processor:
        try:
            yield root.callRemote('add', 42, b=93)
        except spread_provider.RemoteError as e:
            self.assertEquals(e.remoteType, reflect.fullyQualifiedName(exceptions.UnprovidedResourceError))
        else:
            self.fail('UnprovidedResourceError not raised.')

        # call a method when waiting for the processor
        self.server.wait_for_processor = True
        adding = root.callRemote('add', 42, b=93)

        processor_dependency.on_resource_ready(self.processor)
        processor_dependency.fire_on_ready()

        baton = yield self.processor.batons.get()

        # the call must be completed before we complete the test:
        baton['deferred'].callback(42+93)
        yield adding

    @defer.inlineCallbacks
    def test_default_callbacks(self):
        self.create_pb_server()

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        # connect to the server
        client = pb.PBClientFactory()
        reactor.connectTCP('localhost', self.port, client)

        root = yield client.getRootObject()
        self.addCleanup(root.broker.transport.loseConnection)

        # call a remote function
        adding = root.callRemote('add', 42, b=93)

        # assert that the baton is on the expected form
        baton = yield self.processor.batons.get()
        self.assertEquals(baton['message'], 'add')
        self.assertEquals(baton['args'], (42,))
        self.assertEquals(baton['kwargs'], dict(b=93))

        # remove our reference to the baton
        del baton

        # since the server no longer has any reference to the deferred, it will be garbage
        # collected and the client should receive an exception:
        try:
            yield adding
        except spread_provider.RemoteError as e:
            self.assertEquals(e.remoteType, reflect.fullyQualifiedName(exceptions.MissingCallback))
        else:
            self.fail('MissingCallback not raised.')

        # set a default callback and call a remote function
        self.server.default_callback = 'a default callback'
        adding = root.callRemote('add', 42, b=93)

        # we avoid keeping a reference to this baton in order to trigger the default
        yield self.processor.batons.get()

        # wait for the result to come in, which should be our default callback
        result = yield adding
        self.assertEquals(result, self.server.default_callback)

    @defer.inlineCallbacks
    def test_default_callbacks_after_failure(self):
        self.create_pb_server()

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        # connect to the server
        client = pb.PBClientFactory()
        reactor.connectTCP('localhost', self.port, client)

        root = yield client.getRootObject()
        self.addCleanup(root.broker.transport.loseConnection)

        # call a remote function
        adding = root.callRemote('add', 42, b=93)

        # we raise an exception and collect it in a failure because it causes
        # f_locals to be reachable
        # make sure the baton is reachable from the failure frames:
        baton = yield self.processor.batons.get()
        try:
            raise Exception()
        except Exception as e:
            from twisted.python import failure
            f = failure.Failure()

        # remove our reference to the baton and failure
        del baton
        del f
        # since we're in an inlineCallbacks, update our locals so the above del statements can take effect
        # this is explained in more detail in PipedPBService.handle_baton
        locals()

        # since the server no longer has any reference to the deferred, it will be garbage
        # collected and the client should receive an exception:
        try:
            yield adding
        except spread_provider.RemoteError as e:
            self.assertEquals(e.remoteType, reflect.fullyQualifiedName(exceptions.MissingCallback))
        else:
            self.fail('MissingCallback not raised.')

    @defer.inlineCallbacks
    def test_require_login(self):
        self.runtime_environment.configure()
        self.create_pb_server(checker=dict(
            name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
            arguments = dict(test_username='test_password')
        ))

        self.application.startService()
        self.addCleanup(self.application.stopService)

        self.server.processor_dependency = self.processor_dependency

        # connect to the server
        client = pb.PBClientFactory()
        reactor.connectTCP('localhost', self.port, client)

        root = yield client.getRootObject()
        self.addCleanup(root.broker.transport.loseConnection)

        # calling a remote function should result in no such method being found:
        try:
            yield root.callRemote('add', 42, b=93)
        except spread_provider.RemoteError as e:
            self.assertEquals(e.remoteType, reflect.fullyQualifiedName(flavors.NoSuchMethod))
        else:
            self.fail('NoSuchMethod not raised.')

        # attempt to login with different bad credentials
        bad_credentials = list()
        bad_credentials.append(credentials.UsernamePassword('wrong', 'wrong'))
        bad_credentials.append(credentials.UsernamePassword('test_username', 'wrong'))
        bad_credentials.append(credentials.UsernamePassword('wrong', 'test_password'))

        for bad_credential in bad_credentials:
            try:
                yield client.login(bad_credential)
            except spread_provider.RemoteError as e:
                self.assertEquals(e.remoteType, reflect.fullyQualifiedName(error.UnauthorizedLogin))
            else:
                self.fail('NoSuchMethod not raised.')

        perspective = yield client.login(credentials.UsernamePassword('test_username', 'test_password'))

        adding = perspective.callRemote('add', 42, b=93)

        # assert that the baton is on the expected form
        baton = yield self.processor.batons.get()
        self.assertEquals(baton['message'], 'add')
        self.assertEquals(baton['args'], (42,))
        self.assertEquals(baton['kwargs'], dict(b=93))

        # callback the deferred in the baton
        baton['deferred'].callback(42+93)

        # the above callbacking should result in the client getting its response
        result = yield adding
        self.assertEquals(result, 42+93)

class PBClientTest(PBTestBase):
    timeout = 2
    
    @defer.inlineCallbacks
    def test_reconnecting(self):
        self.runtime_environment.configure()
        cm = self.runtime_environment.configuration_manager
        dm = self.runtime_environment.dependency_manager

        self.create_pb_server()
        cm.set('pb.clients.test_client.endpoint', 'tcp:host=localhost:port=%i'%self.port)
        cm.set('pipelines.test_pipeline', [{'eval-lambda': {'lambda': 'baton: baton["deferred"].callback("foo")'}}])
        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        self.application.startService()
        self.addCleanup(self.application.stopService)

        client_provider = spread_provider.PBClientProvider()
        client_provider.configure(self.runtime_environment)

        client_factory_dependency = dm.add_dependency(self, dict(provider='pb.client.test_client'))
        client_root_dependency = dm.add_dependency(self, dict(provider='pb.client.test_client.root_object'))
        dm.resolve_initial_states()

        factory = yield client_factory_dependency.wait_for_resource()
        self.assertIsInstance(factory, spread_provider.PipedPBClientFactory)

        # we set the max_delay in order to make the unit test run quicker
        factory.max_delay = 0

        root = yield client_root_dependency.wait_for_resource()
        self.assertIsInstance(root, pb.RemoteReference)

        yield self.server.stopService()

        # trying to use the root object now should result in an exception
        try:
            yield root.callRemote('omg')
        except pb.PBConnectionLost as e:
            pass
        else:
            self.fail('pb.PBConnectionLost was not raised.')

        # the disconnect event will propagate in _this_ reactor iteration, so wait one
        yield util.wait(0)

        # both dependencies should now be unavailable
        self.assertFalse(client_factory_dependency.is_ready)
        self.assertFalse(client_root_dependency.is_ready)

        # start the server again
        yield self.server.startService()

        # wait for the dependencies to become available again
        new_factory = yield client_factory_dependency.wait_for_resource()
        new_root = yield client_root_dependency.wait_for_resource()

        # the same factory object should be used
        self.assertEquals(new_factory, factory)
        # but the root objects differ
        self.assertNotEquals(new_root, root)

        result = yield new_root.callRemote('anything')
        self.assertEquals(result, 'foo')

    @defer.inlineCallbacks
    def test_valid_login(self):
        self.runtime_environment.configure()
        cm = self.runtime_environment.configuration_manager
        dm = self.runtime_environment.dependency_manager

        self.create_pb_server(checker=dict(
            name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
            arguments = dict(test_username='test_password')
        ))

        cm.set('pb.clients.test_client', dict(
            endpoint = 'tcp:host=localhost:port=%i'%self.port,
            username = 'test_username',
            password = 'test_password'
        ))

        cm.set('pipelines.test_pipeline', [{'eval-lambda': {'lambda': 'baton: baton["deferred"].callback("logged in as %s"%baton["avatar_id"])'}}])
        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        self.application.startService()
        self.addCleanup(self.application.stopService)

        client_provider = spread_provider.PBClientProvider()
        client_provider.configure(self.runtime_environment)

        client_root_dependency = dm.add_dependency(self, dict(provider='pb.client.test_client.root_object'))
        dm.resolve_initial_states()

        root = yield client_root_dependency.wait_for_resource()
        result = yield root.callRemote('any')
        self.assertEquals(result, 'logged in as test_username')

    @defer.inlineCallbacks
    def test_invalid_login(self):
        self.runtime_environment.configure()
        cm = self.runtime_environment.configuration_manager
        dm = self.runtime_environment.dependency_manager

        self.create_pb_server(checker=dict(
            name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
            arguments = dict(test_username='test_password')
        ))

        cm.set('pb.clients.test_wrong_password_client', dict(
            endpoint = 'tcp:host=localhost:port=%i'%self.port,
            username = 'test_username',
            password = 'wrong_password'
        ))

        cm.set('pipelines.test_pipeline', ['passthrough'])

        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        self.application.startService()
        self.addCleanup(self.application.stopService)

        client_provider = spread_provider.PBClientProvider()
        client_provider.configure(self.runtime_environment)

        self.replace_twisted_log_err()

        client_wrong_password_factory_dependency = dm.add_dependency(self, dict(provider='pb.client.test_wrong_password_client'))
        dm.resolve_initial_states()

        # trying to log in with an unauthorized login should result in an "unauthorized login"
        wrong_pw_factory = yield client_wrong_password_factory_dependency.wait_for_resource()
        (args, kwargs) = yield wrong_pw_factory.on_failed_getting_root_object.wait_until_fired()
        reason = args[0]

        self.assertEquals(reason.type, 'twisted.cred.error.UnauthorizedLogin')

    @defer.inlineCallbacks
    def test_manual_login(self):
        self.runtime_environment.configure()
        cm = self.runtime_environment.configuration_manager
        dm = self.runtime_environment.dependency_manager

        self.create_pb_server(checker=dict(
            name = 'twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse',
            arguments = dict(test_username='test_password')
        ))

        cm.set('pb.clients.test_no_login', dict(
            endpoint = 'tcp:host=localhost:port=%i'%self.port,
        ))

        cm.set('pipelines.test_pipeline', [{'eval-lambda': {'lambda': 'baton: baton["deferred"].callback("logged in as %s"%baton["avatar_id"])'}}])
        pp = pipeline_provider.PipelineProvider()
        pp.configure(self.runtime_environment)

        self.application.startService()
        self.addCleanup(self.application.stopService)

        client_provider = spread_provider.PBClientProvider()
        client_provider.configure(self.runtime_environment)

        client_no_login_root_dependency = dm.add_dependency(self, dict(provider='pb.client.test_no_login.root_object'))
        dm.resolve_initial_states()

        # if the client does not login, it will get a RemoteReference to a PortalWrapper
        # which can be used to perform the login "manually"
        no_login_root = yield client_no_login_root_dependency.wait_for_resource()

        challenge, challenger = yield no_login_root.callRemote('login', 'test_username')
        perspective = yield challenger.callRemote('respond', pb.respond(challenge, 'test_password'), mind=None)
        result = yield perspective.callRemote('any')
        self.assertEquals(result, 'logged in as test_username')


class PBServerProviderTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.application = self.runtime_environment.application = service.MultiService()

        self.runtime_environment.configure()

    def test_server_is_provided(self):
        cm = self.runtime_environment.configuration_manager
        cm.set('pb.servers.test_server', dict(
            processor = 'pipeline.test_pipeline',
            listen = 'tcp:0'
        ))

        provider = spread_provider.PBServerProvider()
        provider.configure(self.runtime_environment)

        dep = dependencies.ResourceDependency(provider='pb.server.test_server')
        provider.add_consumer(dep)

        server = dep.get_resource()
        # the server instance should depend on the processor resource
        self.assertEquals(server.processor_dependency.provider, 'pipeline.test_pipeline')