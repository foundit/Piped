# Copyright (c) 2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import re
import socket

from cyclone import web, httpclient
from twisted.application import service
from twisted.internet import defer
from twisted.python import filepath, reflect
from twisted.trial import unittest
from piped import processing
from piped.dependencies import ResourceDependency

from piped_cyclone import providers, handlers


class TestHelloHandler(web.RequestHandler):
    def get(self):
        self.finish('get from test hello handler')


class TestRaisingHandler(handlers.DebuggableHandler):
    def get(self):
        foo = 123
        1/0

class FooModule(web.UIModule):
    pass


class TestApplication(web.Application):
    pass


class CycloneProviderTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.service = service.IService(self.runtime_environment.application)
        self.dependency_manager = self.runtime_environment.dependency_manager
        self.configuration_manager = self.runtime_environment.configuration_manager
        self.resource_manager = self.runtime_environment.resource_manager

        self.dependency_manager.configure(self.runtime_environment)

    def tearDown(self):
        if self.service.running:
            self.service.stopService()

    def get_free_port(self):
        sock = socket.socket()
        sock.bind(('', 0))
        free_port = sock.getsockname()[1]
        sock.close()
        return free_port

    def get_configured_cyclone(self, name='test_cyclone', **config):
        if 'listen' not in config:
            config['listen'] = self.get_free_port()

        resource_name = 'cyclone.application.{0}'.format(name)
        self.configuration_manager.set('cyclone.{0}'.format(name), config)

        provider = providers.CycloneProvider()
        provider.configure(self.runtime_environment)

        provider = self.resource_manager.get_provider_or_fail(resource_name)
        dependency = ResourceDependency(provider=resource_name)
        provider.add_consumer(dependency)

        return dependency.get_resource()

    def test_invalid_handlers(self):
        invalid_handler_configs = [
            dict(foo='bar'), # missing "handler" and "pattern"
            dict(handler='bar'), # missing "pattern"
            dict(pattern='bar'), # missing "handler"
            dict(handler=set()) # unknown handler type
        ]

        for invalid_handler_config in invalid_handler_configs:
            self.assertRaises(providers.InvalidHandlerError, self.get_configured_cyclone,
                listen=8888,
                application = dict(
                    handlers = [
                        invalid_handler_config
                    ]
                )
            )

    @defer.inlineCallbacks
    def test_simple(self):
        port = self.get_free_port()
        app = self.get_configured_cyclone(
            listen = port,
            application=dict(
                handlers=[
                    ('/', reflect.fullyQualifiedName(TestHelloHandler))
                ],

                templates_path = filepath.FilePath('.'),
                static_path = filepath.FilePath('.'),

                ui_modules = dict(
                    foo = reflect.fullyQualifiedName(FooModule)
                )
            )
        )
        self.assertIsInstance(app, web.Application)


        # the _paths in the setting should have been rewritten from FilePaths to strings
        self.assertIsInstance(app.settings.templates_path, basestring)
        self.assertIsInstance(app.settings.static_path, basestring)

        # the foo module should be resolved to an instance
        self.assertIdentical(app.settings.ui_modules['foo'], FooModule)

        # start the web application
        yield self.service.startService()

        response = yield httpclient.fetch('http://localhost:{0}'.format(port))

        self.assertEqual(response.body, 'get from test hello handler')
        self.assertEqual(response.code, 200)

    def test_custom_app(self):
        app = self.get_configured_cyclone(
            type = reflect.fullyQualifiedName(TestApplication)
        )
        self.assertIsInstance(app, TestApplication)

    @defer.inlineCallbacks
    def test_with_pipeline(self):
        class Provider:
            batons = list()

            def add_consumer(self, resource_dependency):
                resource_dependency.on_resource_ready(self)

            def __call__(self, baton):
                self.batons.append(baton)
                baton['handler'].set_status(201)
                baton['handler'].finish('hello from the pipeline')

        provider = Provider()
        self.resource_manager.register('pipeline.foo', provider)

        port = self.get_free_port()
        app = self.get_configured_cyclone(
            listen = port,
            application=dict(handlers=[
                ('/pipeline/?(?P<rest>.*)', dict(provider='pipeline.foo'))
            ])
        )
        self.dependency_manager.resolve_initial_states()

        # start the web application
        yield self.service.startService()

        response = yield httpclient.fetch('http://localhost:{0}/pipeline/123'.format(port))

        self.assertEqual(response.body, 'hello from the pipeline')
        self.assertEqual(response.code, 201)

        self.assertEqual(len(provider.batons), 1)
        self.assertIsInstance(provider.batons[0]['handler'], handlers.PipelineRequestHandler)

        self.assertEqual(provider.batons[0]['kwargs'], dict(rest=u'123'))

    @defer.inlineCallbacks
    def test_with_dependency(self):
        class Provider(web.RequestHandler):
            @classmethod
            def add_consumer(cls, resource_dependency):
                resource_dependency.on_resource_ready(cls)

            def get(self):
                self.set_status(202)
                self.finish('hello from the foo provider')

        self.resource_manager.register('a.dependency.foo', Provider)

        port = self.get_free_port()
        app = self.get_configured_cyclone(
            listen = port,
            application=dict(
                handlers=[
                    ('/dependency', dict(provider='a.dependency.foo'))
                ],
                piped_dependencies = dict(
                    foo = 'a.dependency.foo'
                )
            )
        )
        self.dependency_manager.resolve_initial_states()

        # assert that the piped_dependencies is available in the application settings
        self.assertEqual(app.settings.piped_dependencies.foo, Provider)

        # start the web application
        yield self.service.startService()

        response = yield httpclient.fetch('http://localhost:{0}/dependency'.format(port))

        self.assertEqual(response.body, 'hello from the foo provider')
        self.assertEqual(response.code, 202)

    @defer.inlineCallbacks
    def test_debugging(self):
        port = self.get_free_port()
        app = self.get_configured_cyclone(
            listen = port,
            application=dict(
                handlers=[
                    ('/', reflect.fullyQualifiedName(TestRaisingHandler))
                ],
                debug = True,
                debug_allow = ['127.0.0.1'],
                debug_timeout = 10
            )
        )

        # start the web application
        yield self.service.startService()

        try:
            # perform the web request with debugging enabled so we can inspect the stack :)
            defer.setDebugging(True)

            response = yield httpclient.fetch('http://localhost:{0}/'.format(port))
        finally:
            defer.setDebugging(False)

        self.assertEqual(response.code, 500)
        self.assertIn('web.Application Traceback (most recent call last)', response.body)
        self.assertIn('exceptions.ZeroDivisionError', response.body)

        debugger_id = re.compile(r'.*__debug__=(?P<id>.*)".*').search(response.body).groupdict()['id']

        # in the context, "foo" has been repr'ed to a string by failure.Failure.cleanFailure(). this does not
        # happen if piped -D is used, which replaces the failure class with a subclass that skips the cleaning.
        #
        # exec "foo + ''.join(reversed(foo))", which should return 123321 as a json-encoded string
        exec_response = yield httpclient.fetch('http://localhost:{0}/?__debug__={1}&frame_no=-1&expr=foo%2B"".join(reversed(foo))'.format(port, debugger_id), method='POST')
        self.assertEqual(exec_response.body, '"\'123321\'\\n"')

        # clean out the debugger timeout, so the test does not result in an unclean reactor. this is
        # a lot faster than waiting for the debug_timeout to occur
        delayed_call = TestRaisingHandler._debugging_timeouts.pop(debugger_id, None)
        if delayed_call:
            delayed_call.cancel()

    @defer.inlineCallbacks
    def test_debugging_allow_list(self):
        port = self.get_free_port()
        app = self.get_configured_cyclone(
            listen = port,
            application=dict(
                handlers=[
                    ('/', reflect.fullyQualifiedName(TestRaisingHandler))
                ],
                debug = True,
                debug_allow = [],
                debug_timeout = 0 # setting the timeout to 0 means the debugger will disappear right after the request is done.
            )
        )

        # start the web application
        yield self.service.startService()

        response = yield httpclient.fetch('http://localhost:{0}/'.format(port))

        # we should get an error..
        self.assertEqual(response.code, 500)
        # .. but should be unable to debug because we're not in the debug_allow list.
        self.assertNotIn('web.Application Traceback (most recent call last)', response.body)
        self.assertIn('500: Internal Server Error', response.body)