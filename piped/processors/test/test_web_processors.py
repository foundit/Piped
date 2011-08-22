# coding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
import urlparse
from StringIO import StringIO

import mock
from twisted.trial import unittest
from twisted.internet import defer
from twisted.web import http

from piped import exceptions
from piped.processors import web_processors
from piped.providers import web_provider


class StubRequest(object):

    def __init__(self):
        self.headers = dict()
        self.code = None
        self.data = None
        self.finished = False

        self.channel = Ellipsis

    def setResponseCode(self, code):
        self.code = code

    def setHeader(self, key, value):
        self.headers[key] = value

    def getHeader(self, key):
        return self.headers.get(key)

    def getClientIP(self):
        return 'client-ip'

    def write(self, data):
        self.data = data

    def finish(self):
        self.finished = True


class TestResponseWriter(unittest.TestCase):

    def test_response_code(self):
        codes = dict()

        # We test one code from each group of 100
        codes['SWITCHING'] = 101
        codes['OK'] = 200
        codes['MULTIPLE_CHOICE'] = 300
        codes['BAD_REQUEST'] = 400
        codes['INTERNAL_SERVER_ERROR'] = 500

        for code_name, code_value in codes.items():
            processor_str_code = web_processors.ResponseWriter(response_code=code_name)
            processor_int_code = web_processors.ResponseWriter(response_code=code_value)
            self.assertEquals(processor_str_code.response_code, code_value)
            self.assertEquals(processor_int_code.response_code, code_value)

        self.assertRaises(exceptions.ConfigurationError, web_processors.ResponseWriter, response_code='NONEXISTING HTTP CODE')

    def test_simple_processing(self):
        request = StubRequest()
        processor = web_processors.ResponseWriter()
        processor.process(dict(request=request, content='some data'))

        self.assertEquals(request.data, 'some data')
        self.assertTrue(request.finished)

    def test_encoding(self):
        request = StubRequest()
        processor = web_processors.ResponseWriter()
        processor.process(dict(request=request, content=u'¡some data!'))

        self.assertEquals(request.data, u'¡some data!'.encode('utf8'))
        self.assertTrue(request.finished)

    def test_fallback(self):
        request = StubRequest()
        processor = web_processors.ResponseWriter(fallback_content='some data')
        processor.process(dict(request=request))

        self.assertEquals(request.data, 'some data')

class TestSetHttpHeaders(unittest.TestCase):

    def test_setting_expected_headers(self):
        request = StubRequest()
        expected_headers = dict(x_found='was-here', content_type='text/plain')
        processor = web_processors.SetHttpHeaders(headers=expected_headers)
        baton = dict(request=request)

        processor.process(baton)
        self.assertEqual(request.headers, expected_headers)

    def test_not_removing_existing_headers(self):
        request = StubRequest()
        request.headers['existing'] = 'leave me be'
        expected_headers = dict(x_found='was-here', content_type='text/plain')
        processor = web_processors.SetHttpHeaders(headers=expected_headers)
        baton = dict(request=request)

        processor.process(baton)
        expected_headers['existing'] = 'leave me be'
        self.assertEqual(request.headers, expected_headers)

    def test_overwriting_existing_headers(self):
        request = StubRequest()
        request.headers['existing'] = 'do not leave me be'
        expected_headers = dict(x_found='was-here', content_type='text/plain', existing='overwritten')
        processor = web_processors.SetHttpHeaders(headers=expected_headers)
        baton = dict(request=request)

        processor.process(baton)
        expected_headers['existing'] = 'overwritten'
        self.assertEqual(request.headers, expected_headers)


class TestSetExpireHeader(unittest.TestCase):

    def test_setting_the_right_headers(self):
        request = StubRequest()
        processor = web_processors.SetExpireHeader(timedelta=dict(days=1, hours=1, minutes=1, seconds=1))

        pretended_today = datetime.datetime(2011, 4, 1)
        with mock.patch('piped.processors.web_processors.datetime.datetime') as mocked_datetime:
            mocked_datetime.now.return_value = pretended_today
            processor.process(dict(request=request))

        expected_headers = {'cache-control': 'public,max-age=90061', 'expires': 'Sat, 02 Apr 2011 01:01:01'}
        self.assertEqual(request.headers, expected_headers)


class TestIPDeterminer(unittest.TestCase):

    def test_getting_client_ip(self):
        request = StubRequest()
        processor = web_processors.IPDeterminer()

        baton = dict(request=request)
        processor.process(baton)

        self.assertEquals(baton['ip'], 'client-ip')

    def test_getting_proxied_ip(self):
        request = StubRequest()
        request.setHeader('x-forwarded-for', 'some proxy')
        processor = web_processors.IPDeterminer(proxied=True)

        baton = dict(request=request)
        processor.process(baton)

        self.assertEquals(baton['ip'], 'some proxy')

    def test_getting_proxied_ip_with_custom_header(self):
        request = StubRequest()
        request.setHeader('not-exactly-x-forwarded-for', 'some other proxy')
        processor = web_processors.IPDeterminer(proxied=True, proxy_header='not-exactly-x-forwarded-for')

        baton = dict(request=request)
        processor.process(baton)

        self.assertEquals(baton['ip'], 'some other proxy')

    def test_getting_proxied_ip_with_no_proxy_header(self):
        request = StubRequest()
        processor = web_processors.IPDeterminer(proxied=True)

        baton = dict(request=request)
        processor.process(baton)

        self.assertEquals(baton['ip'], 'client-ip')

    def test_failing_when_getting_proxied_ip_with_no_proxy_header_and_should_fail(self):
        request = StubRequest()
        processor = web_processors.IPDeterminer(proxied=True, fail_if_not_proxied=True)

        baton = dict(request=request)

        exc = self.assertRaises(exceptions.PipedError, processor.process, baton)
        self.assertEquals(exc.msg, 'could not determine IP from proxy-header')


class TestExtractRequestArguments(unittest.TestCase):

    def get_processor(self, **kwargs):
        return web_processors.ExtractRequestArguments(**kwargs)

    @defer.inlineCallbacks
    def test_simple_extract(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', 42)
        request.args['bar'] = [1, 2] # a multivalued argument

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'bar'])
        yield processor.process(baton)

        # only the first value of 'bar' is used by default:
        self.assertEquals(baton, dict(request=request, foo=42, bar=1))

    @defer.inlineCallbacks
    def test_get_all_arguments(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', 42)
        request.args['bar'] = [1, 2] # a multivalued argument

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', dict(bar=dict(only_first=False))])
        yield processor.process(baton)

        # all values of bar should be returned as a list:
        self.assertEquals(baton, dict(request=request, foo=42, bar=[1,2]))

    @defer.inlineCallbacks
    def test_nonexistent(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', 42)

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'nonexistent'])
        yield processor.process(baton)

        # the nonexistent value should be skipped
        self.assertEquals(baton, dict(request=request, foo=42))

    @defer.inlineCallbacks
    def test_nonexistent_without_skipping(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', 42)

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'nonexistent'], skip_if_nonexistent=False)
        yield processor.process(baton)

        # the nonexistent value should be replaced by the default fallback
        self.assertEquals(baton, dict(request=request, foo=42, nonexistent=None))

    @defer.inlineCallbacks
    def test_nonexistent_with_fallback(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', 42)

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'nonexistent'], input_fallback='fallback', skip_if_nonexistent=False)
        yield processor.process(baton)

        # the nonexistent value should be replaced by the fallback
        self.assertEquals(baton, dict(request=request, foo=42, nonexistent='fallback'))

    @defer.inlineCallbacks
    def test_loading_json(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', 42)
        request.args['bar'] = ['{"loaded": 42}', '{"loaded": 93}'] # a multivalued argument containing json

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', dict(bar=dict(load_json=True, only_first=False))])
        yield processor.process(baton)

        self.assertEquals(baton, dict(request=request, foo=42, bar=[dict(loaded=42), dict(loaded=93)]))

    @defer.inlineCallbacks
    def test_loading_invalid_json(self):
        request = web_provider.DummyRequest([])
        request.addArg('foo', '')

        baton = dict(request=request)
        processor = self.get_processor(mapping=[dict(foo=dict(load_json=True))])

        try:
            yield processor.process(baton)
            self.fail('Expected ValueError to be raised.')
        except Exception as e:
            self.assertIsInstance(e, ValueError)
            self.assertEquals(e.args, ('No JSON object could be decoded',))


class TestProxyForward(unittest.TestCase):

    def _create_processor(self, **config):
        processor = web_processors.ProxyForward(**config)
        return processor

    @defer.inlineCallbacks
    def test_simple_forward(self):
        request = web_provider.DummyRequest([''])
        request.requestHeaders.setRawHeaders('foo', ['foo-header'])
        processor = self._create_processor(url='http://proxied:81')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                try:
                    self.assertEquals(host, 'proxied')
                    self.assertEquals(port, 81)
                    # we do not want the processor to automatically forward the data received from the
                    # remote server because we might want to post-process the response before responding
                    # to our incoming request:
                    self.assertNotEquals(factory.father, request)
                    self.assertEquals(factory.rest, '/')
                    self.assertEquals(factory.headers, dict(foo='foo-header', host='proxied:81'))
                finally:
                    factory.father.finish()
            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))
            self.assertIsInstance(baton['proxied_request'], web_provider.DummyRequest)
            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_forward_post_with_data(self):
        request = web_provider.DummyRequest([''])
        request.method = 'POST'
        request.set_content('this is some post data')
        processor = self._create_processor(url='http://proxied:81')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                try:
                    transport = StringIO()
                    proto = factory.buildProtocol(None)
                    proto.transport = transport
                    proto.connectionMade()

                    transport.seek(0)

                    expected_sent = [
                        'POST / HTTP/1.0',
                        'host: proxied:81',
                        'connection: close',
                        '',
                        'this is some post data'
                    ]
                    self.assertEquals(transport.read().split('\r\n'), expected_sent)

                finally:
                    factory.father.finish()
            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))
            self.assertIsInstance(baton['proxied_request'], web_provider.DummyRequest)
            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_forward_with_query_string(self):
        request = web_provider.DummyRequest([''])
        request.args['foo_q'] = ['one', 'two']
        request.args['bar_q'] = ['1']
        request.args['empty'] = ['']
        processor = self._create_processor(url='http://proxied:81')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                try:
                    self.assertEquals(host, 'proxied')
                    self.assertEquals(port, 81)
                    self.assertNotEquals(factory.father, request)

                    parsed = http.parse_qs(urlparse.urlparse(factory.rest).query, keep_blank_values=True)
                    self.assertEquals(parsed, dict(
                        foo_q = ['one', 'two'],
                        bar_q = ['1'],
                        empty = ['']
                    ))
                    self.assertEquals(factory.rest, '/?foo_q=one&foo_q=two&bar_q=1&empty=')
                finally:
                    factory.father.finish()
            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))
            self.assertIsInstance(baton['proxied_request'], web_provider.DummyRequest)
            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_headers_and_response_code_set(self):
        request = web_provider.DummyRequest(['bar', 'baz'])
        request.uri = '/foo/bar/baz'
        request.requestHeaders.setRawHeaders('host', ['proxy:80'])

        processor = self._create_processor(url='http://proxied:81/foo')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                proto = factory.buildProtocol(None)
                self.assertEquals(factory.rest, '/foo/bar/baz')
                lines = [
                    'HTTP/1.1 200 OK',
                    'my-header: this is a header',
                    '',
                    'some result'
                ]
                for line in lines:
                    proto.dataReceived(line+'\r\n')

                factory.father.finish()

            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))
            proxied_request = baton['proxied_request']
            self.assertEquals(proxied_request.responseHeaders.getRawHeaders('my-header'), ['this is a header'])
            self.assertEquals(proxied_request.written, ['some result'+'\r\n'])
            self.assertEquals(proxied_request.code, 200)
            self.assertEquals(proxied_request.code_message, 'OK')

            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_remaining_path_elements_used(self):
        request = web_provider.DummyRequest(['bar', 'baz'])
        processor = self._create_processor(url='http://proxied:81/foo')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                try:
                    self.assertEquals(factory.rest, '/foo/bar/baz')
                finally:
                    factory.father.finish()
            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))
            self.assertIsInstance(baton['proxied_request'], web_provider.DummyRequest)

            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_redirect_with_location_rewriting(self):
        request = web_provider.DummyRequest(['bar', 'baz'])
        request.uri = '/foo/bar/baz'
        request.requestHeaders.setRawHeaders('host', ['proxy:80'])

        processor = self._create_processor(url='http://proxied:81/foo')
        processor.consumers.append('foo')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                proto = factory.buildProtocol(None)
                self.assertEquals(factory.rest, '/foo/bar/baz')
                lines = [
                    'HTTP/1.1 302 Moved Temporarily',
                    'Location: http://proxied:81/foo/',
                    '',
                ]
                for line in lines:
                    proto.dataReceived(line+'\r\n')

                factory.father.finish()

            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))
            self.assertEquals(baton, Ellipsis)
            self.assertEquals(request.responseHeaders.getRawHeaders('location'), ['http://proxy:80/foo/'])

            # the original request should have received the redirect
            self.assertEquals(request.code, 302)
            self.assertEquals(request.code_message, 'Moved Temporarily')

            # after a redirect, the processor should try to stop further processing on the baton
            self.assertEquals(processor.get_consumers(baton), list())

            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_redirect_outside_proxy_without_stopping(self):
        """ Redirects to outside of this proxy should not be rewritten. """
        request = web_provider.DummyRequest(['bar', 'baz'])
        request.uri = '/foo/bar/baz'
        request.requestHeaders.setRawHeaders('host', ['proxy:80'])

        processor = self._create_processor(url='http://proxied:81/foo', stop_if_redirected=False)
        processor.consumers.append('foo')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                proto = factory.buildProtocol(None)
                self.assertEquals(factory.rest, '/foo/bar/baz')
                lines = [
                    'HTTP/1.1 302 Moved Temporarily',
                    'Location: http://proxied:81/another',
                    '',
                ]
                for line in lines:
                    proto.dataReceived(line+'\r\n')

                factory.father.finish()

            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))

            # since the proxied server responded with a redirect outside the processors scope, it should not have been rewritten:
            self.assertEquals(baton['proxied_request'].responseHeaders.getRawHeaders('location'), ['http://proxied:81/another'])

            # since we created the processor with stop_if_redirected=False, the processor should not attempt to
            # stop further processing after the rewriting:
            self.assertEquals(processor.get_consumers(baton), ['foo'])

            self.assertEquals(mocked_connect.call_count, 1)

    @defer.inlineCallbacks
    def test_disabled_redirect_rewriting(self):
        request = web_provider.DummyRequest(['bar', 'baz'])
        request.uri = '/foo/bar/baz'
        request.requestHeaders.setRawHeaders('host', ['proxy:80'])

        processor = self._create_processor(url='http://proxied:81/foo', rewrite_redirects=False)
        processor.consumers.append('foo')

        with mock.patch('twisted.internet.reactor.connectTCP') as mocked_connect:
            def verify(host, port, factory):
                proto = factory.buildProtocol(None)
                self.assertEquals(factory.rest, '/foo/bar/baz')
                lines = [
                    'HTTP/1.1 302 Moved Temporarily',
                    'Location: http://proxied:81/foo/',
                    '',
                ]
                for line in lines:
                    proto.dataReceived(line+'\r\n')

                factory.father.finish()

            mocked_connect.side_effect = verify

            baton = yield processor.process(dict(request=request))

            # the original request should have gotten the redirect the server sent
            self.assertEquals(request.code, 302)
            self.assertEquals(request.code_message, 'Moved Temporarily')
            self.assertEquals(request.responseHeaders.getRawHeaders('location'), ['http://proxied:81/foo/'])

            self.assertEquals(mocked_connect.call_count, 1)


class TestRequestChainer(unittest.TestCase):

    def _create_processor(self, **config):
        processor = web_processors.RequestChainer(**config)
        return processor

    def test_chaining(self):
        from_request = web_provider.DummyRequest([''])
        from_request.setResponseCode(123, 'response message')
        from_request.setHeader('foo-header', 'foo-header-value')
        from_request.write('this is some data')
        from_request.finish()

        to_request = web_provider.DummyRequest([''])

        processor = self._create_processor()
        processor.process(dict(request=to_request, proxied_request=from_request))

        self.assertEquals(to_request.code, 123)
        self.assertEquals(to_request.code_message, 'response message')
        self.assertEquals(to_request.responseHeaders.getRawHeaders('foo-header'), ['foo-header-value'])
        self.assertEquals(to_request.written, ['this is some data'])
        self.assertTrue(to_request.finished)

    def test_finishing(self):
        from_request = web_provider.DummyRequest([''])
        from_request.finish()

        to_request = web_provider.DummyRequest([''])

        processor = self._create_processor(finish=False)
        processor.process(dict(request=to_request, proxied_request=from_request))

        self.assertFalse(to_request.finished)
