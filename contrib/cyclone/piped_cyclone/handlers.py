# Copyright (c) 2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import os
import json

from cyclone import web
from zope import interface
from twisted.internet import defer, reactor
from twisted.web import util
from twisted.python import failure

from piped import debugger, log


class DebuggableHandler(web.RequestHandler):
    """ A request handler that lets allowed users view full tracebacks and execute
    code interactively to inspect the traceback frames.

    Subclasses must make sure that :meth:`.prepare()` is called. If you want to require login
    in addition to the built-in ip-based checks, subclass this class and override
    :meth:`~DebuggableHandler.post_debug` and :meth:`~DebuggableHandler.get_debug`::

        class AuthingDebuggableHandler(web.RequestHandler):
            @web.authenticated
            def post_debug(self, *a, **kw):
                super(AuthingDebuggableHandler, self).post_debug(*a, **kw)

            @web.authenticated
            def get_debug(self, *a, **kw):
                super(AuthingDebuggableHandler, self).get_debug(*a, **kw)
    """
    _debugging_failures = dict()
    _debugging_timeouts = dict()

    def prepare(self):
        """ Override this request handlers methods if the user tries to access a debugger. """
        if self.settings.get('debug', False) and self.get_argument('__debug__', None):
            self.post = self.post_debug
            self.get = self.get_debug

    def post_debug(self, *a, **kw):
        """ Executes an expression within a debugger frame. """
        debugger = self.get_debugger(self.get_argument('__debug__', None), timeout=self.settings.get('debug_timeout', 60))
        if not debugger:
            raise web.HTTPError(404, 'No such debugger.')

        allowed = self.settings.get('debug_allow', list())

        if not self.request.remote_ip in allowed:
            raise web.HTTPError(403)

        expr = self.get_argument('expr', None)
        frame_no = self.get_argument('frame_no', None)

        result = debugger.exec_expr(expr, int(frame_no))
        self.finish(json.dumps(result))

    def get_debug(self, *a, **kw):
        """ Returns a HTML-page that serves as a front-end to the debugger. """
        debugger = self.get_debugger(self.get_argument('__debug__', None), timeout=self.settings.get('debug_timeout', 60))

        if not debugger or not self.request.remote_ip in self.settings.get('debug_allow', list()):
            raise web.HTTPError(404, 'No such debugger.')

        self.finish(self._get_debug_html(debugger.failure))

    def write_error(self, status_code, **kwargs):
        if not self.settings.get('debug', False):
            super(DebuggableHandler, self).write_error(status_code, **kwargs)
            return

        # store the current failure reason
        reason = None

        if 'exception' in kwargs and isinstance(kwargs['exception'], failure.Failure):
            reason = kwargs['exception']

        if 'exc_info' in kwargs:
            try:
                raise exc_info[0], exc_info[1], exc_info[2]
            except Exception as e:
                reason = failure.Failure()

        if reason:
            self.register_failure(reason, timeout=self.settings.get('debug_timeout', 60))

            ajax_endpoint = self.request.path + '?__debug__='+str(id(reason))
            full_url_to_debugger = self.request.protocol + '://' + self.request.host + ajax_endpoint

            log.error('Debugger for [{0}] started at [{1}]'.format(reason.getErrorMessage(), full_url_to_debugger))

            self.request.arguments['__debug__'] = [str(id(reason))]
            if self.request.remote_ip in self.settings.get('debug_allow', list()):
                return self.get_debug()

        super(DebuggableHandler, self).write_error(status_code, **kwargs)

    def _get_debug_html(self, reason):
        traceback_as_html = util.formatFailure(reason)
        ajax_endpoint = self.request.path + '?__debug__='+str(id(reason))

        here = os.path.abspath(os.path.dirname(__file__))

        template_name = self.settings.get('debug_template', os.path.join(here, 'templates', 'debugger.html'))

        try:
            return self.render_string(template_name, traceback_as_html=traceback_as_html, ajax_endpoint=ajax_endpoint)
        except Exception as e:
            log.error()
            raise e

    @classmethod
    def register_failure(cls, reason, timeout):
        key = str(id(reason))
        cls._debugging_failures[key] = debugger.Debugger(reason)

        cls._debugging_timeouts[key] = reactor.callLater(timeout, cls._timeout_debugger, key)

    @classmethod
    def get_debugger(cls, id, timeout):
        debugger = cls._debugging_failures.get(id, None)
        delayed_call = cls._debugging_timeouts.pop(id, None)

        if delayed_call:
            delayed_call.cancel()
            cls._debugging_timeouts.pop(id, None)
            cls._debugging_timeouts[id] = reactor.callLater(timeout, cls._timeout_debugger, id)

        return debugger

    @classmethod
    def _timeout_debugger(cls, id):
        cls._debugging_failures.pop(id, None)
        cls._debugging_timeouts.pop(id, None)


class PipedRequestHandlerProxy(object):
    """ A simple proxy that uses a dependency to process web requests.

    Assumes the provided resource acts like a :class:`cyclone.web.RequestHandler`.
    """

    def __init__(self, dependency):
        self.dependency = dependency

    def __call__(self, *args, **kwargs):
        self.handler_args = args
        self.handler_kwargs = kwargs
        return self

    @defer.inlineCallbacks
    def _execute(self, *a, **kw):
        handler_class = yield self.dependency.wait_for_resource()
        handler = handler_class(*self.handler_args, **self.handler_kwargs)
        yield handler._execute(*a, **kw)


class PipelineRequestHandler(DebuggableHandler):
    """ Request handler for requests that will be served through pipelines. """

    def __init__(self, *args, **kwargs):
        super(PipelineRequestHandler, self).__init__(*args, **kwargs)

        for method in self.SUPPORTED_METHODS:
            setattr(self, method.lower(), self.handle)

    def initialize(self, dependency=None):
        self.dependency = dependency

    @web.asynchronous
    @defer.inlineCallbacks
    def handle(self, *args, **kwargs):
        pipeline = yield self.dependency.wait_for_resource()
        pipeline(dict(handler=self, args=args, kwargs=kwargs))