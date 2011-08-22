# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
import json
import urlparse
import urllib
import urllib2

from twisted.web import http
from twisted.internet import reactor, defer
from twisted.web import proxy
from zope import interface

from piped import util, exceptions, processing
from piped.processors import base
from piped.providers import web_provider


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


class ExtractRequestArguments(base.MappingProcessor):
    """ Extract arguments from a :class:`twisted.web.server.Request`-like object.

    The input paths in the mapping is lookup up in the request arguments and
    copied to the specified output paths.

    The mapping support the following additional keywords:

        only_first
            Only returns the first request argument by that name. Defaults to True.

        load_json
            Causes the value to be loaded as json before being copied into the baton.
            Defaults to False.

    Consider the following example configuration:

    .. code-block:: yaml

        mapping:
            - foo
            - bar:
                only_first: false
            - baz:
                load_json: true
            - zip:
                output_path: zap

    Using the above configuration to extract the request arguments of a request to
    ``http://.../?foo=1&foo=2&bar=3&bar=4&baz={"test":[5,6,7]}&zip=8`` results in the following baton:

    .. code-block:: yaml

        request: <Request object>
        foo: '1'
        bar: ['1', '2']
        baz:
            test: [5, 6, 7]
        zap: '8'

    Note that the integers in the request are not parsed. For more advanced input validation, see
    the :ref:`validate-with-formencode` processor.

    """
    interface.classProvides(processing.IProcessor)
    name = 'extract-web-request-arguments'

    def __init__(self, request_path='request', *a, **kw):
        """
        :param request_path: Path to the request object in the baton.
        :param skip_if_nonexistent: Whether to skip mapping entries that are not found in the request.
        """
        super(ExtractRequestArguments, self).__init__(*a, **kw)

        self.request_path = request_path

    def get_input(self, baton, input_path, **kwargs):
        request = util.dict_get_path(baton, self.request_path)
        return request.args.get(input_path, Ellipsis)

    def process_mapping(self, input, input_path, output_path, baton, only_first=True, load_json=False):
        # we have to recheck if the input_path is in the request arguments, otherwise we don't know
        # whether the input is a default provided by our configuration or an actual argument.
        request = util.dict_get_path(baton, self.request_path)
        if input_path not in request.args:
            return input

        if load_json:
            for i, value in enumerate(input):
                input[i] = json.loads(value)

        if only_first:
            return input[0]

        return input


class ProxyForward(HttpRequestProcessor):
    """ Forwards requests to another server.

    When proxying requests, this processor will append the remaining segments in ``request.postpath``
    to the destination url.

    Redirects that are children of the destination url are rewritten to use the current url as a prefix. It
    accomplishes this by calculating a base url based on the incoming request, and replacing the configured
    url (if found in the new location header) with its own base url.

    For example, given the following processor configuration:

    .. code-block:: yaml

        url: http://proxied:81

    If the url used to access this processor is ``http://proxy`` and the destination server responds with a
    redirect response code and the location header set to: "http://proxied:81/bar/", this proxy will send the
    same response code with the location header set to ``http://proxy/bar/`` instead. If this processor gets
    a redirect from the remote server, it will forward this redirect to the incoming request and try to stop
    further processing of the request by not using any consumer processors.

    .. note:: The proxied url cannot currently be a HTTPS url.

    .. warning:: This processor buffers the entire response from the proxied server.
    """

    interface.classProvides(processing.IProcessor)
    name = 'proxy-forward'
    proxy_client_factory = proxy.ProxyClientFactory

    def __init__(self, url, noisy=False, proxied_request_path='proxied_request', rewrite_redirects=True, stop_if_redirected=True, *a, **kw):
        """
        :param url: The destination url to forward the request to
        :param noisy: Whether the proxy protocol should be noisy or not. Defaults to false.
        :param proxied_request_path: Path to where the proxied request object should stored.
        :param rewrite_redirects: Whether to rewrite redirects.
        :param stop_if_redirected: Attempt to stop processing on this baton if the proxied request was redirected.
        """
        super(ProxyForward, self).__init__(*a, **kw)

        self.url = url
        self.noisy = noisy
        self.proxied_request_path = proxied_request_path
        self.rewrite_redirects = rewrite_redirects
        self.stop_if_redirected = stop_if_redirected

    @defer.inlineCallbacks
    def process_request(self, request, baton):
        proxy_url = self.get_input(baton, self.url)
        remaining_path = '/'.join(urllib2.quote(segment, safe='') for segment in request.postpath)
        base_url_here = self._construct_base_url_here(request, proxy_url, remaining_path)
        proxied_url_parsed = urlparse.urlparse(proxy_url)

        proxied_path = self._create_proxied_request_path(proxied_url_parsed, remaining_path)
        proxied_query_string = self._create_query_string(request)

        rest = proxied_path + proxied_query_string

        # TODO: support for streaming large files etc.
        # create the proxied request object and add it to the baton
        proxied_request = web_provider.DummyRequest(request.postpath)
        util.dict_set_path(baton, self.proxied_request_path, proxied_request)

        # rewind the original requests content buffer before reading the data from it, since another processor
        # might have read its contents.
        request.content.seek(0, 0)
        data = request.content.read()

        headers = request.getAllHeaders()
        # we need to set the 'host' header, as our host can differ from the proxied host
        headers['host'] = proxied_url_parsed.netloc

        clientFactory = self.proxy_client_factory(request.method, rest, request.clientproto, headers, data, proxied_request)
        clientFactory.noisy = self.noisy

        reactor.connectTCP(proxied_url_parsed.hostname, proxied_url_parsed.port or 80, factory=clientFactory)

        # wait until the proxied request is finished:
        if not proxied_request.finished:
            yield proxied_request.notifyFinish()

        # We attempt to rewrite redirects coming from the proxied server in order to try to get the redirected request
        # to use this proxy too.
        if proxied_request.code in (301, 302, 303, 307):
            if self.rewrite_redirects:
                location = proxied_request.responseHeaders.getRawHeaders('location')[-1]
                proxied_request.responseHeaders.setRawHeaders('location', [location.replace(self.url, base_url_here)])

            if self.stop_if_redirected:
                for key, values in proxied_request.responseHeaders.getAllRawHeaders():
                    request.responseHeaders.setRawHeaders(key, values)

                request.setResponseCode(proxied_request.code, proxied_request.code_message)
                defer.returnValue(Ellipsis)

        defer.returnValue(baton)

    def get_consumers(self, baton):
        if baton is Ellipsis:
            return list()

        return super(ProxyForward, self).get_consumers(baton)

    def _create_query_string(self, request):
        if request.args:
            return '?'+urllib.urlencode(request.args, doseq=True)
        return ''

    def _construct_base_url_here(self, request, proxy_url, remaining_path):
        """ Construct the url used to create the given request. """
        scheme = 'http'
        if request.isSecure():
            scheme = 'https'

        current_host = request.getHeader('host')
        # the request might have remaining path elements, and these should be stripped
        # off the path when constructing the base url
        uri = urlparse.urlparse(request.uri).path # parse the request.uri as it may or may not contain some host/port information in addition to the path
        current_path = uri[:len(uri)-len(remaining_path)]

        # make sure the proxy url and the current path has the same number of
        if current_path.endswith('/') and not proxy_url.endswith('/'):
            current_path = current_path[:-1]

        # we don't care about params, the query string or fragments in the base url
        base_url = urlparse.urlunparse((scheme, current_host, current_path, '', '', ''))
        return base_url

    def _create_proxied_request_path(self, proxied_url_parsed, remaining_path):
        path = proxied_url_parsed.path

        # the path must start with a forward slash since we are using it in the http request
        if not path.startswith('/'):
            path = '/' + path

        # use the default path if we have nothing to append
        if not remaining_path:
            return path

        # if we have something to append, make sure that we don't get double slashes:
        if not path.endswith('/'):
            return path + '/' + remaining_path

        return path + remaining_path


class RequestChainer(base.Processor):
    """ Chains two web requests together.

    The headers, response code, message and currently written data (if available) are copied.
    """
    interface.classProvides(processing.IProcessor)
    name = 'chain-web-requests'

    def __init__(self, from_request='proxied_request', to_request='request', finish=True, *a, **kw):
        """
        :param from_request: The request to copy from.
        :param to_request: The request to copy to.
        :param finish: Whether to finish the to_request if the from_request is finished.
        """
        super(RequestChainer, self).__init__(*a, **kw)

        self.to_request = to_request
        self.from_request = from_request
        self.finish = finish

    def process(self, baton):
        to_request = util.dict_get_path(baton, self.to_request)
        from_request = util.dict_get_path(baton, self.from_request)

        # write the headers
        for key, values in from_request.responseHeaders.getAllRawHeaders():
            to_request.responseHeaders.setRawHeaders(key, values)

        # copy the response code
        to_request.setResponseCode(from_request.code, from_request.code_message)

        # copy the written data if they're available
        if hasattr(from_request, 'written'):
            for data in from_request.written:
                to_request.write(data)

        if self.finish and from_request.finished:
            to_request.finish()

        return baton