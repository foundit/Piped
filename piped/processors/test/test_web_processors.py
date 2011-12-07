# coding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
from StringIO import StringIO

import mock
from twisted.trial import unittest
from twisted.internet import defer

from piped import exceptions
from piped.processors import web_processors
from piped.providers.test import test_web_provider


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
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', 42)
        request.args['bar'] = [1, 2] # a multivalued argument

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'bar'])
        yield processor.process(baton)

        # only the first value of 'bar' is used by default:
        self.assertEquals(baton, dict(request=request, foo=42, bar=1))

    @defer.inlineCallbacks
    def test_get_all_arguments(self):
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', 42)
        request.args['bar'] = [1, 2] # a multivalued argument

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', dict(bar=dict(only_first=False))])
        yield processor.process(baton)

        # all values of bar should be returned as a list:
        self.assertEquals(baton, dict(request=request, foo=42, bar=[1,2]))

    @defer.inlineCallbacks
    def test_nonexistent(self):
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', 42)

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'nonexistent'])
        yield processor.process(baton)

        # the nonexistent value should be skipped
        self.assertEquals(baton, dict(request=request, foo=42))

    @defer.inlineCallbacks
    def test_nonexistent_without_skipping(self):
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', 42)

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'nonexistent'], skip_if_nonexistent=False)
        yield processor.process(baton)

        # the nonexistent value should be replaced by the default fallback
        self.assertEquals(baton, dict(request=request, foo=42, nonexistent=None))

    @defer.inlineCallbacks
    def test_nonexistent_with_fallback(self):
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', 42)

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', 'nonexistent'], input_fallback='fallback', skip_if_nonexistent=False)
        yield processor.process(baton)

        # the nonexistent value should be replaced by the fallback
        self.assertEquals(baton, dict(request=request, foo=42, nonexistent='fallback'))

    @defer.inlineCallbacks
    def test_loading_json(self):
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', 42)
        request.args['bar'] = ['{"loaded": 42}', '{"loaded": 93}'] # a multivalued argument containing json

        baton = dict(request=request)
        processor = self.get_processor(mapping=['foo', dict(bar=dict(load_json=True, only_first=False))])
        yield processor.process(baton)

        self.assertEquals(baton, dict(request=request, foo=42, bar=[dict(loaded=42), dict(loaded=93)]))

    @defer.inlineCallbacks
    def test_loading_invalid_json(self):
        request = test_web_provider.DummyRequest([])
        request.addArg('foo', '')

        baton = dict(request=request)
        processor = self.get_processor(mapping=[dict(foo=dict(load_json=True))])

        try:
            yield processor.process(baton)
            self.fail('Expected ValueError to be raised.')
        except Exception as e:
            self.assertIsInstance(e, ValueError)
            self.assertEquals(e.args, ('No JSON object could be decoded',))


class TestClientGetPage(unittest.TestCase):

    def _create_processor(self, **config):
        processor = web_processors.ClientGetPage(**config)
        return processor

    def test_arguments(self):
        processor = self._create_processor(base_url='http://example.com/', timeout=lambda: defer.succeed(80),
            postdata=StringIO('my data'), headers=lambda: dict(foo='bar'), cookies=defer.succeed(dict(cookies='yum')))

        with mock.patch.object(web_processors, 'client') as mocked_client:
            mocked_client.getPage.return_value = 'mocked page'

            baton = yield processor.process(dict(url=['foo', 'bar', 'baz']))

            self.assertEquals(mocked_client.getPage.call_count, 1)

            args, kwargs = mocked_client.getPage.call_args_list[0]

            # we avoid using positional arguments
            self.assertEquals(args, ())
            # the base url was used, and the url was flattened
            self.assertEquals(kwargs['url'], 'http://example.com/foo/bar/baz')
            # the postdata was extracted from a buffer
            self.assertEquals(kwargs['postdata'], 'my data')
            # headers were retrieved from a callable
            self.assertEquals(kwargs['headers'], dict(foo='bar'))
            # the timeout comes from an async function
            self.assertEquals(kwargs['timeout'], 80)
            # cookies comes from a deferred
            self.assertEquals(kwargs['cookies'], dict(cookies='yum'))

            # the resulting page is set in the baton
            self.assertEquals(baton['page'], 'mocked page')