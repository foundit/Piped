# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import json
import datetime
import weakref

from zope import interface
from twisted.internet import defer, task
from twisted.web import server, resource, static, util as web_util, http, http_headers
from twisted.web.test import test_web
from twisted.application import service, internet, strports

from piped import exceptions, log, util, debugger
from piped import resource as piped_resource

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


STANDARD_HTML_TEMPLATE = """
    <html><head><title>Processing Failed</title></head><body><b>Processing Failed</b></body></html>
"""

DEBUG_HTML_TEMPLATE = r"""
<html>
    <head>
        <title>web.Server Traceback (most recent call last)</title>
            <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.5.0/jquery.min.js"></script>
            <script type="text/javascript">
$(document).ready(function() {
    var ajax_endpoint = "%(ajax_endpoint)s";

    $('.frame,.firstFrame').each(function(i, item) {
        // Make the location div clickable
        var location = $(item).find('.location');
        location.click(function() {
            location.siblings().toggle();
            $(item).find('input').focus();
        });
        location.css('cursor', 'pointer');

        // create the buffer and input divs
        var buffer = $('<div class="buffer"></div>');
        var input = $('<input class="expr monospaced"/>');
        var input_div = $('<div><span class="prompt monospaced">&#62;&#62;&#62;</span></div>');
        input_div.css('width', location.width());
        buffer.css('width', '100%%');

        // variables for the command history
        var cmd_history = [];
        var cmd_current = 0;

        input.bind('keydown', function(e) {
            var key = e.keyCode || e.which;
            if(key == 13 && input.val()) { // enter
                var expr = input.val();

                // update the command history
                cmd_history.push(expr);
                cmd_current = cmd_history.length;
                input.val('');

                // execute the expression in the debugger
                $.ajax({
                    url: ajax_endpoint,
                    dataType: 'json',
                    data: {
                        frame_no: i,
                        expr: expr
                    },
                    success: function(data, textStatus, jqXHR) {
                        buffer.append($('<div class="monospaced"/>').text('>>> '+expr));
                        buffer.append($('<div class="monospaced"/>').text(data));
                        // set the font-family of the new elements.
                        buffer.find('.monospaced').css('font-family', 'monospace');
                        input.focus();
                    },
                    error: function(jqXHR, textStatus, errorThrown) {
                        var result_div = $('<div style="background: red"/>')
                        if(jqXHR.status == 404) {
                            result_div.text('Got a 404 when performing the ajax request. This either means the debugger \
                                                was reaped due to inactivity on the server side, or that this page is \
                                                trying to use the wrong endpoint. If the debugger was reaped, consider \
                                                adjusting the inactive time.');
                        } else {
                            result_div.text('Unknown response from the server: '+jqXHR.status+': '+textStatus);
                        }
                        buffer.append(result_div);
                    }
                });
            } else if (key == 38)  { // up arrow fetches the previous command from the history
                cmd_current = Math.max(0, cmd_current-1);
                input.val(cmd_history[cmd_current]);
            } else if (key == 40) { // down arrow fetches the next command from the command history
                cmd_current = Math.min(cmd_history.length, cmd_current+1);
                if(cmd_current == cmd_history.length) {
                    input.val('');
                } else {
                    input.val(cmd_history[cmd_current]);
                }
            }
        });

        // add our divs to the current stack frame
        $(item).append(buffer);
        input_div.append(input);
        $(item).append(input_div);

        // set the input width
        input.css('width', input_div.width()-input_div.find('.prompt').width());

        $(item).find('.monospaced').css('font-family', 'monospace');
    });
    $('.location').click(); // start with all the details hidden
});
        </script>
    </head>
    <body><b>web.Server Traceback (most recent call last):</b><div>%(body)s</div></body>
</html>
"""


# we use a global cache of proxy classes to avoid creating a new class per request
# instead of per request type.
_proxy_classes = dict()

def _get_proxy_class(request):
    """ Return a class that is a subclass of the requests class. """
    cls = request.__class__
    if cls not in _proxy_classes:
        class RequestProxy(cls):
            def __init__(self, request):
                self.__dict__ = request.__dict__
                self.__request = request

            def __eq__(self, other):
                return self.__request == other

            # since we're overriding __eq__ we must override __hash__:
            def __hash__(self):
                return hash(self.__request)

            def finish(self):
                return self.__request.finish()

        _proxy_classes[cls] = RequestProxy

    return _proxy_classes[cls]

def get_request_proxy(request):
    """ Get a proxy object to the given request. """
    cls = _get_proxy_class(request)
    instance = cls(request)
    return instance


class WebResourceProvider(object, service.MultiService):
    """ Provides HTTP interfaces.

    .. highlight:: yaml
    
    Example configuration::

        web:
            my_site: # logical name of the web service
                enabled: true/false
                ... # the rest of the keys/values are used by the :class:`WebSite`

    """
    interface.classProvides(piped_resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setName('web')
        self.setServiceParent(runtime_environment.application)

        self.sites = runtime_environment.get_configuration_value('web', dict())

        for site_name, site_configuration in self.sites.items():
            if not site_configuration.get('enabled', True):
                continue
            website = WebSite(site_name, site_configuration)
            website.setName(site_name)
            website.setServiceParent(self)
            website.configure(runtime_environment)


class WebSite(object, service.MultiService):
    """ A website in Piped.

    Example site configuration::
    
        my_site:
            port: 8080
            listen: ssl:1234 # overrides the "port" above, see :mod:`twisted.application.strports`
            log_exceptions: DEBUG # a piped.log debug level, or null. (default: null)
            debug: # configure debugging of the processors
                reap_interval: 60 # seconds
                max_inactive_time: 300 # seconds
                allow: # list of hostnames or ip addresses that are allowed to debug
                    - localhost
            routing:
                # mapping that is used to look up resources based on the traversed url.
                # this is given to :class:`WebResource`
    """

    def __init__(self, site_name, site_configuration):
        service.MultiService.__init__(self)
        self.site_name = site_name
        self.site_configuration = site_configuration

        self.log_exceptions_level = site_configuration.get('log_exceptions', None)

        self.debug_configuration = util.dict_get_path(self.site_configuration, 'debug', dict())

    def startService(self):
        service.MultiService.startService(self)

    def stopService(self):
        service.MultiService.stopService(self)

    def configure(self, runtime_environment):
        routing = self.site_configuration['routing']

        if self.debug_configuration:
            log.debug('Debugging enabled for site "%s": %s.' % (self.site_name, self.debug_configuration))
            if not self.debug_configuration['allow']:
                log.warn('No clients are currently allowed to debug on site "%s".' % self.site_name)

        root_resource = WebResource(self, routing)
        root_resource.configure(runtime_environment)

        self.factory = server.Site(root_resource)
        self.factory.displayTracebacks = bool(self.debug_configuration)

        listen = self.site_configuration.get('listen', str(self.site_configuration.get('port', 8080)))
        self.tcpserver = strports.service(listen, self.factory)
        self.tcpserver.setServiceParent(self)

    def log_exception(self, failure):
        if not self.log_exceptions_level:
            return
        logger = getattr(log, self.log_exceptions_level)
        logger(failure)


class StaticFile(static.File):
    indexNames = ['index.html'] # we do not want anything but index.html to be used as an index, especially not 'index'

    def __init__(self, path, defaultType="text/html", ignoredExts=(), registry=None, allowExt=0, namespace=None, preprocessors=None, **kwargs):
        static.File.__init__(self, util.expand_filepath(path), defaultType=defaultType, ignoredExts=ignoredExts, registry=registry, allowExt=allowExt, **kwargs)

        self.namespace = dict(self=self)
        self.namespace.update(namespace or dict())
        self.preprocessor_definitions = preprocessors or dict()

        self.preprocessors = list()
        for lambda_definition in self.preprocessor_definitions.values():
            func = util.create_lambda_function(lambda_definition, **self.namespace)
            self.preprocessors.append(func)

    def render_GET(self, request):
        if self.exists() and not self.isdir():
            for preprocessor in self.preprocessors:
                preprocessor(request)

        return static.File.render_GET(self, request)

    def createSimilarFile(self, path):
        f = static.File.createSimilarFile(self, path)
        f.preprocessors = self.preprocessors
        return f


class ConcatenatedFile(resource.Resource):
    """ Resource that renders the concatenation of the configured
    *file_path*s, with the specified *content_type*. """

    def __init__(self, content_type, file_paths):
        resource.Resource.__init__(self)
        self.content_type = content_type
        self.file_paths = file_paths

    def render_GET(self, request):
        request.setHeader('content-type', self.content_type)
        return ''.join(self._get_concatenated_lines())

    def _get_concatenated_lines(self):
        buf = list()
        for fp in self.file_paths:
            file = open(util.expand_filepath(fp))
            buf.extend(file.readlines())
        return buf


class WebDebugger(resource.Resource):
    """ A JSON endpoint for debugging. """

    def __init__(self, failure):
        resource.Resource.__init__(self)
        self.failure = failure
        self.debugger = debugger.Debugger(failure)

    def render(self, request):
        expr = request.args.get('expr', [''])[0]
        frame_no = int(request.args.get('frame_no', ['-1'])[0])
        result = self.debugger.exec_expr(expr, frame_no)
        return json.dumps(result)

    def should_be_reaped(self, max_inactive_time):
        now = datetime.datetime.now()
        inactive_time = now - self.debugger.last_command
        if inactive_time > datetime.timedelta(0, max_inactive_time):
            return True
        return False


class WebDebugHandler(resource.Resource, service.Service):
    """ Maintains multiple web debugging sessions.

    This class takes care of reaping them after a configurable amount of inactive time.
    """

    def __init__(self, site, allow=None, max_inactive_time=300, reap_interval=60):
        resource.Resource.__init__(self)
        self.site = site
        self.forbidden = resource.ForbiddenResource()

        self.allow = allow or list()
        self.max_inactive_time = max_inactive_time
        self.reap_interval = reap_interval

    def startService(self):
        service.Service.startService(self)
        self.reaper = task.LoopingCall(self._reap_old_debuggers)
        self.reaper.start(interval=self.reap_interval)

    def stopService(self):
        service.Service.stopService(self)
        if self.reaper.running:
            self.reaper.stop()

    def _reap_old_debuggers(self):
        for web_debugger in self.children.values():
            if web_debugger.should_be_reaped(self.max_inactive_time):
                self.unregister_failure(web_debugger.failure)

    def removeChild(self, path):
        child = self.children[path]
        del self.children[path]
        child.server = None

    def _get_path_from_failure(self, failure):
        return str(id(failure))

    def unregister_failure(self, failure):
        path = self._get_path_from_failure(failure)
        self.removeChild(path)

    def register_failure(self, failure):
        """ Registers a failure for debugging, and returns a path to the endpoint
        relative to this resource. """
        path = self._get_path_from_failure(failure)
        self.putChild(path, WebDebugger(failure))
        return path

    def is_client_allowed_debugging(self, request):
        """ Checks with the debugging configuration if the client responsible for
        the request is allowed to perform debugging. """
        client_names = [request.getClient(), request.getClientIP()]
        for client_name in client_names:
            if client_name in self.allow:
                return True
        formatted_client_name = ' or '.join(['"%s"' % client_name for client_name in list(set(client_names))])
        log.debug('A client identified by %s attempted to access a debugging resource, but was denied.' % formatted_client_name)
        return False

    def getChildWithDefault(self, path, request):
        if self.is_client_allowed_debugging(request):
            return resource.Resource.getChildWithDefault(self, path, request)
        return self.forbidden


class WebResource(resource.Resource):
    """ A routed web resource.

    Example routing configuration::

        my_site:
            routing:
                __config__:
                    processor: processor_name # name of processor to run. the processor receives a baton containig the request
                nested:
                    __config__:
                        debug:
                            allow: [] # disables debugging of this processor, overriding the site-wide configuration
                        oricessir: another_processor_name
                sparse:
                    __config__:
                        no_resource_processor: sparse_processor
                js:
                    __config__:
                        static: ~/js
                    foo:
                        __config__:
                            processor: foo_processor
                            static:
                                path: ~/js/foo
                                namespace:
                                    now: datetime.datetime.utcnow
                                    delta: datetime.timedelta
                                preprocessors:
                                    expires: "request: request.setHeader('expires', (now()+delta(seconds=8600)).strftime('%a, %d %b %Y %H:%M:%S UTC'))"
                                    cache-control: "request: request.setHeader('cache-control', 'public,max-age=8600')"


    The __config__ may contain the following keys:

    processor
        Makes a processor available at this resource. Accessing this resource directly causes the request
        object to be passed into the specified processor. The baton is on the form:

        .. code-block:: python

            baton = dict(request=request_object)

        The processor is expected to call .finish() on the request when the processing is complete. If the
        processing raises an Exception, the request will be closed automatically and debugging will
        become available if debugging is enabled and the client is allowed to debug.

    no_resource_processor
        Works similar to the ``processor`` configuration key, but is used when another resource was not found for
        request for either this path or any children. If this processor is used to handle child paths without
        any explicit resources, the request instance will contain a non-empty ``postpath`` instance variable, which
        is a list of child path elements, relative to its location in the routing.

        This can be used to create a sparse web site routing.

    static
        Makes static resources such as files and directories available under this resource. This option
        may be a mapping on the form:

        .. code-block:: yaml

            path: some_path # required
            namespace: # optional, namespace used for the preprocessors
                time: time
            preprocessors: # a dict of name -> preprocessor definition
                logical_name: "request: request.setHeader('serviced-at', time.time())"

    concatenated
        Creates a virtually concatenated file:

        .. code-block:: yaml

            file_paths:
                - file_a.js
                - file_b.js
            content_type: text/javascript

    debug
        Overrides the site-wide debug option for this processor. Only applies if a processor is specified.

    Accessing the following resources with the above configuration gives:

    - http://hostname:port/ will put a invoke the processor ``processor_name``.
    - http://hostname:port/nested/ will put a invoke the processor ``another_processor``.
    - http://hostname:port/sparse/ will put a invoke the processor ``sparse_processor``.
    - http://hostname:port/sparse/some/path will put a invoke the processor ``sparse_processor``.
    - http://hostname:port/js/ will show a directory listing of ``~/js``
    - http://hostname:port/js/foo will put a invoke the processor ``foo_processor``

    """
    no_resource = resource.NoResource()
    static_resource = None
    debug_handler = None
    processor_dependency = None
    no_resource_processor_dependency = None

    def __init__(self, site, routing):
        resource.Resource.__init__(self)
        self.site = site

        # store a copy since we'll be modifying it
        self.routing = routing.copy()
        # store the original because we might want to introspect it
        self.original_routing = routing

        self._weakrefs = set()

    def configure(self, runtime_environment):
        self._fail_if_routing_is_invalid()

        resource_configuration = self.routing.pop('__config__', dict())
        self._configure_resource(runtime_environment, **resource_configuration)

        for child_path, child_config in self.routing.items():
            child = WebResource(self.site, child_config)
            child.configure(runtime_environment)

            self.putChild(child_path, child)

    def _configure_resource(self, runtime_environment, processor=None, no_resource_processor=None, debug=None, static=None, concatenated=None):
        dependency_manager = runtime_environment.dependency_manager

        debug_configuration = dict(self.site.debug_configuration)
        debug_overrides = debug or dict()
        debug_configuration.update(debug_overrides)

        # add ourselves to the dependency-graph if we have any dependent processors:
        if processor or no_resource_processor:
            if debug_configuration:
                self.debug_handler = WebDebugHandler(self.site, **debug_configuration)
                self.debug_handler.setServiceParent(self.site)
                self.putChild('__debug__', self.debug_handler)

            dependency_manager.add_dependency(self, self.site)

        if processor:
            self.putChild('', self)
            self.processor_dependency = dependency_manager.add_dependency(self, dict(provider=processor) if isinstance(processor, basestring) else processor)

        if no_resource_processor:
            # if the same processor handles both the regular rendering and the "no resource" rendering, we reuse the dependency:
            if no_resource_processor == processor:
                self.no_resource_processor_dependency = self.processor_dependency
            else:
                self.no_resource_processor_dependency = dependency_manager.add_dependency(self, dict(provider=no_resource_processor) if isinstance(no_resource_processor, basestring) else no_resource_processor)

        if static is not None:
            if not isinstance(static, dict):
                static = dict(path=static)

            self.static_resource = StaticFile(**static)

        if concatenated is not None:
            self.static_resource = ConcatenatedFile(**concatenated)

    def _fail_if_routing_is_invalid(self):
        if not isinstance(self.routing, dict):
            raise exceptions.ConfigurationError('The routing must be a dictionary.')

        config = self.routing.get('__config__', dict())
        if not config:
            return
        
        if 'static' in config and 'concatenated' in config:
            raise exceptions.ConfigurationError('Both static and concatenated specified')

    def _process_baton_with_processor(self, baton, processor_dependency):
        processor = processor_dependency.get_resource()
        return processor(baton)

    def getChild(self, path, request):
        # this function is only called after the static routing is finished
        # so we only need to check if we have a static resource at this point.
        if self.static_resource:
            return self.static_resource.getChild(path, request)
        return resource.NoResource('No such resource.')

    def getChildWithDefault(self, path, request):
        # try to find a child resource
        child_resource = resource.Resource.getChildWithDefault(self, path, request)

        # if we do not have any "no resource" processor, we cannot handle request
        if not self.no_resource_processor_dependency:
            return child_resource

        # we set the relative postpath on the request instance since our caller
        # (usually resource.getChildForRequest) only considers itself done once
        # request.postpath is empty. we restore this in our render() method.
        relative_postpath = [path] + list(request.postpath)

        # if a "no resource" is found, none of our direct children is willing to handle
        # this request. if this request is looking for a leaf resource (request.postpath
        # being empty), we can serve this request.
        if isinstance(child_resource, resource.NoResource):
            request.relative_postpath = relative_postpath
            request.postpath = list()
            return self

        # if there are more segments left to resolve in the request path, we try to find
        # the deepest possible resource in the configuration that is able to handle
        # the request.

        child_resource = resource.getChildForRequest(child_resource, request)

        # if the resource found is a non-404 resource, we use it:
        if not isinstance(child_resource, resource.NoResource):
            if not isinstance(child_resource, WebResource):
                return child_resource

            # if the child resource has any processors at all, it can render the request:
            if child_resource.processor_dependency or child_resource.no_resource_processor_dependency:
                return child_resource

        # otherwise, we have exhausted all options of finding a resource for this request, and
        # since we are able to render these missing resources, return ourselves.
        request.relative_postpath = relative_postpath
        return self

    def render(self, request):
        """ Runs the request through the provided processor or returns 404
        if this resource has no configured processor.
        """
        # restore the relative postpath of the request if we have any:
        request.postpath = getattr(request, 'relative_postpath', request.postpath)

        # We send a proxy of the request into the processor in order to be able to
        # see if the request proxy gets garbage collected before the request is
        # finished.
        request_proxy = get_request_proxy(request)
        processor_dependency = self._get_processor_dependency_for_request(request)
        
        if not processor_dependency:
            # If we don't have a processor to render, see if we have a static_resource to render
            if self.static_resource:
                return self.static_resource.render(request)
            # No processor or static_resource for this resource, so consider it non-existing.
            return self.no_resource.render(request)

        baton = dict(request=request_proxy)
        d = defer.maybeDeferred(self._process_baton_with_processor, baton, processor_dependency)
        d.addErrback(self._delayed_errback, request=request_proxy)
        d.addErrback(log.error)
        # The end result of this deferred cannot contain a reference to the request_proxy in any
        # way, since that will affect the garbage collection of the request_proxy. Because of this,
        # we always replace its final callback/errback result with None, after any error handling and
        # logging has finished.
        d.addBoth(lambda _: None)

        # we want to ensure that the client gets an response, so we add an callback that will
        # be called when the request we provided in the baton are garbage collected. when it
        # is finalized, we make sure that the client has gotten a response
        ref = weakref.ref(request_proxy, lambda ref: self._handle_deferred_gc(ref, request))
        self._weakrefs.add(ref)

        if not request.finished:
            request.notifyFinish().addErrback(self._delayed_cancelled, deferred=d)

        return server.NOT_DONE_YET

    def _handle_deferred_gc(self, ref, request):
        """ This function is called when the request_proxy in the baton have been finalized. """
        # if the client haven't gotten a response, just finish the request
        log.debug('Garbage collecting for request: %s'%request)
        if not request.finished and request.channel:
            request.finish()
        self._weakrefs.remove(ref)

    def _get_error_body(self, request, failure):
        if self.debug_handler and self.debug_handler.is_client_allowed_debugging(request):
            ajax_endpoint = request.childLink('__debug__/' + self.debug_handler.register_failure(failure))

            debug_variables = dict(ajax_endpoint=ajax_endpoint,
                                   body=web_util.formatFailure(failure))

            html = DEBUG_HTML_TEMPLATE % debug_variables
            return html

        return STANDARD_HTML_TEMPLATE

    def _delayed_errback(self, failure, request):
        if failure.check(defer.CancelledError):
            # The deferred was cancelled, most likely because the client abandoned the request, so do nothing.
            return

        self.site.log_exception(failure)

        body = self._get_error_body(request, failure)

        request.setResponseCode(http.INTERNAL_SERVER_ERROR)
        request.setHeader('content-type', 'text/html')
        request.setHeader('content-length', str(len(body)))

        request.write(body)
        request.finish()

    def _delayed_cancelled(self, failure, deferred):
        # Attempt to cancel further processing on the deferred, if possible
        if not deferred.called:
            deferred.cancel()

    def _get_processor_dependency_for_request(self, request):
        # if there are non-empty elements left in request.postpath, we're handling the request on behalf of
        # a resource that is missing.
        if '/'.join(request.postpath):
            return self.no_resource_processor_dependency

        # if we do not have a processor dependency, we consider ourselves to be a "no resource" and
        # render ourselves with the no_resource processor.
        if not self.processor_dependency:
            return self.no_resource_processor_dependency

        return self.processor_dependency
