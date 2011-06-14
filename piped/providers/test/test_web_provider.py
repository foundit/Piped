# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json

from twisted.application import service
from twisted.internet import defer, address
from twisted.python import filepath, failure
from twisted.trial import unittest
from twisted.web import server, resource
from twisted.web.test import test_web

from piped import exceptions, util, processing, dependencies
from piped.providers import web_provider


class StubPipeline(object):

    def __init__(self, processor):
        self.processor = processor

    def process(self, baton):
        return self.processor(baton)


class DummyRequest(test_web.DummyRequest, server.Request):
    channel = Ellipsis


class WebProviderTest(unittest.TestCase):

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

    def _create_configured_web_resource(self, routing, site_configuration=None):
        site_configuration = site_configuration or dict()
        web_site = web_provider.WebSite('site_name', site_configuration)
        web_resource = web_provider.WebResource(web_site, routing)
        web_resource.configure(self.runtime_environment)
        return web_resource

    def assertConfiguredWithPipeline(self, web_resource, pipeline):
        self.assertEquals(web_resource.has_pipeline, True)
        self.assertEquals(web_resource.pipeline_dependency.provider, pipeline)

    def getResourceForFakeRequest(self, site, post_path):
        return site.factory.getResourceFor(DummyRequest(post_path))

    def getConfiguredWebSite(self, config):
        web_site = web_provider.WebSite('site_name', config)
        web_site.configure(self.runtime_environment)
        return web_site

    def test_enabled_web_sites_provided(self):
        provider = web_provider.WebResourceProvider()
        self.configuration_manager.set('web.my_site.routing',
           dict(__config__=dict(pipeline='a_pipeline'))
        )
        self.configuration_manager.set('web.another_site.enabled', False)
        self.configuration_manager.set('web.another_site.routing',
            dict(__config__=dict(pipeline='a_pipeline'))
        )

        provider.configure(self.runtime_environment)

        self.assertEquals(len(provider.services), 1)

    def test_simple_pipeline_routing(self):
        config = dict(
            routing = dict(
                __config__ = dict(pipeline='a_pipeline')
            )
        )

        web_site = self.getConfiguredWebSite(config)

        web_resource = self.getResourceForFakeRequest(web_site, [''])
        self.assertConfiguredWithPipeline(web_resource, 'pipeline.a_pipeline')

    def test_static_preprocessors(self):
        current_file = filepath.FilePath(__file__)

        config = dict(
            routing = dict(
                __config__ = dict(
                    static = dict(
                        path = current_file.dirname(),
                        preprocessors = dict(
                            foo = "request: request.setHeader('foo', 'bar')"
                        )
                    )
                )
            )
        )

        web_site = self.getConfiguredWebSite(config)

        # send a request for this file:
        request = DummyRequest([current_file.basename()])
        resource = web_site.factory.getResourceFor(request)
        resource.render(request)

        self.assertEquals(request.outgoingHeaders['foo'], 'bar')

    def test_pipeline_routing_with_nested_resources(self):
        config = dict(
            routing = dict(
                __config__ = dict(
                    pipeline = 'a_pipeline',
                    static = filepath.FilePath(__file__).dirname(),
                ),
                nested = dict(
                    deeply = dict(
                        __config__ = dict(
                            pipeline = 'another_pipeline'
                        )
                    )
                )
            )
        )

        web_site = self.getConfiguredWebSite(config)

        web_resource = self.getResourceForFakeRequest(web_site, [''])

        self.assertConfiguredWithPipeline(web_resource, 'pipeline.a_pipeline')

        # if we request an existing file, a static file resource will be returned
        filename = filepath.FilePath(__file__).basename()
        static_resource = self.getResourceForFakeRequest(web_site, [filename])
        self.assertIsInstance(static_resource, web_provider.StaticFile)

        web_resource = self.getResourceForFakeRequest(web_site, ['nested'])
        self.assertEquals(web_resource.has_pipeline, False)

        no_resource = self.getResourceForFakeRequest(web_site, ['nested', 'nonexistent'])
        self.assertIsInstance(no_resource, resource.NoResource)

        deeply_resource = self.getResourceForFakeRequest(web_site, ['nested', 'deeply'])
        self.assertConfiguredWithPipeline(deeply_resource, 'pipeline.another_pipeline')

    def test_web_resource_simple_request_processing(self):
        web_resource = self._create_configured_web_resource(dict(__config__=dict(pipeline='a_pipeline')))

        request = DummyRequest([''])

        batons = list()
        pipeline = StubPipeline(batons.append)
        web_resource.pipeline_dependency.on_resource_ready(pipeline)

        # rendering the request should result in a baton being processed by the pipeline
        web_resource.render(request)
        self.assertEquals(batons, [dict(request=request)])

    def test_web_resource_processing_handles_exceptions(self):
        web_resource = self._create_configured_web_resource(dict(__config__=dict(pipeline='a_pipeline')))

        request = DummyRequest([''])

        def raiser(baton):
            raise Exception()

        pipeline = StubPipeline(raiser)
        web_resource.pipeline_dependency.on_resource_ready(pipeline)

        # rendering the request should result in an exception response
        web_resource.render(request)

        self.assertIn('Processing Failed', ''.join(request.written))
        self.assertEquals(request.responseCode, 500)

    def test_web_resource_processing_raises_with_debugging(self):
        routing = dict(__config__=dict(pipeline='a_pipeline'))
        site_config = dict(debug=dict(allow=['localhost']))
        web_resource = self._create_configured_web_resource(routing, site_config)

        request = DummyRequest([''])
        request.client = address.IPv4Address('TCP', 'localhost', 1234)

        def raiser(baton):
            raise Exception()

        pipeline = StubPipeline(raiser)
        web_resource.pipeline_dependency.on_resource_ready(pipeline)

        # rendering the request should result in an exception response
        web_resource.render(request)

        self.assertIn('web.Server Traceback (most recent call last)', ''.join(request.written))
        self.assertEquals(request.responseCode, 500)

    @defer.inlineCallbacks
    def test_debug_handler_reaping(self):
        # reap all debuggers every reactor iteration:
        site_config = dict(routing=dict())
        web_site = web_provider.WebSite('site_name', site_config)

        debug_handler = web_provider.WebDebugHandler(web_site, reap_interval=0, max_inactive_time=0)
        debug_handler.setServiceParent(self.service)

        self.service.startService()

        f = failure.Failure(Exception())
        debug_handler.register_failure(f)

        self.assertEquals(len(debug_handler.children), 1)
        yield util.wait(0) # give the reaper one reactor iteration to reap the debugger
        self.assertEquals(len(debug_handler.children), 0)

    def test_debug_handler_allow(self):
        site_config = dict(routing=dict())
        web_site = self.getConfiguredWebSite(site_config)

        debug_handler = web_provider.WebDebugHandler(web_site, allow=['some_host'])
        debug_handler.setServiceParent(self.service)

        f = failure.Failure(Exception())
        path = debug_handler.register_failure(f)

        request = DummyRequest([path])

        # localhost is not allowed to debug:
        request.client = address.IPv4Address('TCP', 'localhost', 1234)
        forbidden = debug_handler.getChildWithDefault(path, request)
        self.assertIsInstance(forbidden, resource.ForbiddenResource)

        # but some_host is:
        request.client = address.IPv4Address('TCP', 'some_host', 1234)
        web_debugger = debug_handler.getChildWithDefault(path, request)
        self.assertIsInstance(web_debugger, web_provider.WebDebugger)

    def test_web_debugger(self):
        # create a failure instance with an actual traceback:
        foo = 42 # this will become part of the debuggers namespace
        try:
            raise Exception()
        except Exception as e:
            f = failure.Failure()

        web_debugger = web_provider.WebDebugger(f)

        request = DummyRequest([])
        request.addArg('expr', 'foo')
        result = web_debugger.render(request)

        # the result should be json-encoded
        self.assertEquals(result, json.dumps('42\n'))

    def test_fails_if_both_static_and_concatenated_are_specified(self):
        for invalid_routing in (dict(__config__=dict(static='', concatenated='')),
                               dict(nested=dict(__config__=dict(static='', concatenated='')))):
            site = web_provider.WebSite('site_name', dict(routing=invalid_routing))
            self.assertRaises(exceptions.ConfigurationError, site.configure, self.runtime_environment)

    def test_request_finished_when_garbage_collected(self):
        web_site = web_provider.WebSite('site_name', dict(routing=dict(__config__=dict(pipeline='test_pipeline'))))
        web_site.configure(self.runtime_environment)

        # create a stub pipeline that collects the batons
        batons = list()
        stub_pipeline = StubPipeline(batons.append)

        web_resource = self.getResourceForFakeRequest(web_site, [])
        web_resource.pipeline_dependency = dependencies.InstanceDependency(stub_pipeline)
        web_resource.pipeline_dependency.is_ready = True

        request = DummyRequest([])
        web_resource.render(request)

        # the pipeline should have been asked to process a baton
        self.assertEquals(len(batons), 1)
        self.assertEquals(batons[0]['request'], request)

        # the pipeline didn't finish the request:
        self.assertEquals(request.finished, False)

        # .. however, when the pipeline loses the reference to the request, it should be
        # automatically finished:
        batons.pop()
        self.assertEquals(request.finished, True)


class TestConcatenatedFile(unittest.TestCase):

    def test_concatenating_files(self):
        test_data_path = filepath.FilePath(__file__).sibling('data')
        file_paths = [test_data_path.child('foo'), test_data_path.child('bar')]
        cf = web_provider.ConcatenatedFile('text/plain', file_paths)

        request = DummyRequest([''])
        text = cf.render_GET(request)

        self.assertEquals(text, 'foo\nbar\n')

    def test_concatenating_files_in_different_order(self):
        test_data_path = filepath.FilePath(__file__).sibling('data')
        file_paths = [test_data_path.child('bar'), test_data_path.child('foo')]
        cf = web_provider.ConcatenatedFile('text/plain', file_paths)

        request = DummyRequest([''])
        text = cf.render_GET(request)

        self.assertEquals(text, 'bar\nfoo\n')

    def test_just_a_single_file(self):
        test_data_path = filepath.FilePath(__file__).sibling('data')
        file_paths = [test_data_path.child('foo')]
        cf = web_provider.ConcatenatedFile('text/plain', file_paths)

        request = DummyRequest([''])
        text = cf.render_GET(request)

        self.assertEquals(text, 'foo\n')

    def test_no_files(self):
        file_paths = []
        cf = web_provider.ConcatenatedFile('text/plain', file_paths)

        request = DummyRequest([''])
        text = cf.render_GET(request)

        self.assertEquals(text, '')

    def test_ensure_the_right_content_type_is_set(self):
        file_paths = []
        cf = web_provider.ConcatenatedFile('text/plain', file_paths)

        request = DummyRequest([''])
        cf.render_GET(request)

        self.assertEquals(request.outgoingHeaders['content-type'], 'text/plain')
