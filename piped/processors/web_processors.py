# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime

from twisted.web import http
from zope import interface

from piped import util, exceptions, processing
from piped.processors import base


class HttpRequestProcessor(base.Processor):

    def __init__(self, request_path='request', skip_if_request_stopped=True, **kw):
        super(HttpRequestProcessor, self).__init__(**kw)
        self.request_path = request_path
        self.skip_if_request_stopped = skip_if_request_stopped

    def process(self, baton):
        request = self.get_request_or_fail(baton)

        if self.skip_if_request_stopped and not request.channel:
            return baton

        return self.process_request(request, baton)

    def get_request_or_fail(self, baton):
        request = util.dict_get_path(baton, self.request_path)
        if not request:
            self._fail_because_request_is_invalid()
        return request

    def _fail_because_request_is_invalid(self):
        raise exceptions.PipedError('could not find request at %r' % self.request_path)

    def process_request(self, request, baton):
        raise NotImplementedError()


# TODO: Docs
class ResponseWriter(HttpRequestProcessor):
    """ A processor that writes the response to a twisted.web.server.Request

        :param response_code: Either an integer response code or a string. If a string is
            supplied, it is converted to an integer by looking up the response codes
            defined in twisted.web.http during initialization.
        :type response_code: int or str
    """
    interface.classProvides(processing.IProcessor)
    name = 'write-web-response'

    def __init__(self, content_path='content', content_type=None, response_code=None,
                 encoding='utf8', finish=True, fallback_content=Ellipsis, **kw):
        super(ResponseWriter, self).__init__(**kw)
        self.content_path = content_path
        self.content_type = content_type
        self.finish = finish
        self.fallback_content = fallback_content
        self.encoding = encoding

        if isinstance(response_code, basestring):
            response_code = getattr(http, response_code, Ellipsis)

        self._fail_if_response_code_is_invalid(response_code)
        self.response_code = response_code

    def _fail_if_response_code_is_invalid(self, response_code):
        if response_code is not None and not isinstance(response_code, int):
            e_msg = 'Invalid response code.'
            hint = 'A response code must be either an integer, or a valid string.'
            valid_strings = set()
            for code_name in dir(http):
                code = getattr(http, code_name, Ellipsis)
                if isinstance(code, int) and code in http.RESPONSES:
                    valid_strings.add('"%s"' % code_name)

            detail = 'Valid strings are: %s.' % (','.join(valid_strings))
            raise exceptions.ConfigurationError(e_msg, hint, detail)

    def process_request(self, request, baton):
        content = util.dict_get_path(baton, self.content_path, self.fallback_content)

        if self.response_code:
            request.setResponseCode(self.response_code)
        if self.content_type:
            request.setHeader('content-type', self.content_type)

        if isinstance(content, unicode):
            content = content.encode(self.encoding)

        if content is not Ellipsis:
            request.write(content)

        if self.finish:
            request.finish()

        return baton


class IPDeterminer(HttpRequestProcessor):
    """ Determine the IP of the HTTP-client.

    If *proxied* is true, then the *proxy_header*, which defaults to
    "x-forwarded-for", is used to get the IP.

    If an IP is not found at the proxy header, the client-IP is
    returned --- unless *fail_if_not_proxied* is true, in which case
    a `PipedError` is raised.
    """
    interface.classProvides(processing.IProcessor)
    name = 'determine-ip'

    def __init__(self, output_path='ip',
                 proxied=False, proxy_header='x-forwarded-for', fail_if_not_proxied=False, **kw):
        kw.setdefault('skip_if_request_stopped', False)
        super(IPDeterminer, self).__init__(**kw)
        self.output_path = output_path
        self.proxied = proxied
        self.proxy_header = proxy_header
        self.fail_if_not_proxied = fail_if_not_proxied

    def process_request(self, request, baton):
        ip = self._determine_ip(request)

        util.dict_set_path(baton, self.output_path, ip)

        return baton

    def _determine_ip(self, request):
        if self.proxied:
            # Try to get the IP from the proxy-header
            ip = request.getHeader(self.proxy_header)
            if ip:
                return ip.split(',')[0].strip()

            if self.fail_if_not_proxied:
                e_msg = 'could not determine IP from proxy-header'
                detail = 'The proxy header is "%s"' % self.proxy_header
                hint = ('Ensure the proxy-header is right, set "proxied" to false if the service is '
                        'no longer proxied, or set "fail_if_not_proxied" to false to fallback to the client IP.')
                raise exceptions.PipedError(e_msg, detail, hint)

        return request.getClientIP()


class SetHttpHeaders(HttpRequestProcessor):
    """ Adds *headers* as response headers. """
    interface.classProvides(processing.IProcessor)
    name = 'set-http-headers'

    def __init__(self, headers, **kw):
        super(SetHttpHeaders, self).__init__(**kw)
        self.headers = headers

    def process_request(self, request, baton):
        for name, value in self.headers.items():
            request.setHeader(name, value)

        return baton


class SetExpireHeader(HttpRequestProcessor):
    """ Set cache headers to indicate that the response should be
    cached for *timedelta* seconds.

    :param timedelta: a dictionary with the keys *days*, *hours*,
        *minutes* and *seconds*. The resulting timedelta is the sum of
        these.
    """
    interface.classProvides(processing.IProcessor)
    name = 'set-http-expires'

    def __init__(self, timedelta, **kw):
        super(SetExpireHeader, self).__init__(**kw)
        self.timedelta_kwargs = timedelta

    def process_request(self, request, baton):
        delta = datetime.timedelta(**self.timedelta_kwargs)
        seconds = 86400 * delta.days + delta.seconds
        until = datetime.datetime.now() + delta
        request.setHeader('expires', until.strftime('%a, %d %b %Y %H:%M:%S %Z').strip())
        request.setHeader('cache-control', 'public,max-age=%i' % seconds)

        return baton
