# coding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime

from mock import patch
from twisted.trial import unittest

from piped import exceptions
from piped.processors import web_processors


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
        with patch('piped.processors.web_processors.datetime.datetime') as mocked_datetime:
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
