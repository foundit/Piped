# Copyright (c) 2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from cyclone import web
from zope import interface
from twisted.application import strports
from twisted.python import reflect, filepath

from piped import resource, exceptions
from piped_cyclone import handlers


class InvalidHandlerError(exceptions.PipedError):
    pass


class CycloneProvider(object):
    """ Provides support for running `Cyclone <http://cyclone.io>`_ applications within Piped.

    For more in-depth documentation about Cyclone see: `Cyclone on GitHub <http://github.com/fiorix/cyclone>`_,
    and the `Tornado documentation <http://www.tornadoweb.org/documentation/index.html>`_.

    Configuration example:

    .. code-block:: yaml

        cyclone:
            site_name:
                listen: 8888
                type: cyclone.web.Application # or a fully qualified name of a subclass
                application:
                    handlers:
                        # a tuple: pattern, handler, kwargs (optional), name (optional)
                        - ['/pattern', name.of.RequestHandler]
                        # a dict
                        - pattern: /another/(?P<pattern>.*)
                          handler: name.of.RequestHandler
                          name: handler_name # optional
                          kwargs: # optional
                            foo: 123

                    debug: true # see the debugging section below.
                    debug_allow: # optional list of ip addresses that are allowed to see tracebacks and the interactive debugger.
                        - 127.0.0.1
                    debug_timeout: 60 # (default), time in seconds to keep debuggers resident in memory before garbage collecting them.
                    debug_template: debugger.html  # name of the debugger template to use. if not set, uses the built-in template.

    The ``listen`` parameter is a Twisted ``strport``, which can be used to declare which interfaces the server should be listening on, SSL
    parameters and more. For more information, see `strports in the Twisted documentation
    <http://twistedmatrix.com/documents/11.0.0/api/twisted.internet.endpoints.html#serverFromString>`_.

    Each ``handler`` may be one of the following:

        * Fully qualified class name of a RequestHandler instance. The class will be invoked with
          cls.configure(runtime_environment) once if the method is defined. This allows for the
          class to perform any necessary setup / bootstrapping into the runtime_environment.

        * A dict with a ``class`` key set: Same as the above.

        * A dict with the ``provider`` key set. The provider is assumed to provide a subclass of
          RequestHandler as the resource.

          If the provider starts with ``pipeline.``, the provided resource will not be used as
          a RequestHandler, but will be called with ``resource.__call__(baton)``. The baton contains
          the following keys:

            * handler: A RequestHandler that is handling the request.
            * args: A tuple of arguments from the url pattern
            * kwargs: A dict of keyword arguments from the url pattern

          The pipeline should take care of calling ``baton['handler'].finish()`` when the request is
          finished.

    If the application setting ``ui_modules`` is a string, it will be loaded to a python object via
    :meth:`twisted.python.reflect.namedAny`. If it is a dict, all values are assumed to be strings
    that are fully qualified name of :class:`cyclone.web.UIModule` classes.

    Application settings that end with ``_path`` and are :class:`twisted.python.filepath.FilePath` instances
    will be converted to absolute paths. This enables the use of the :ref:`\!path <path-constructor>`-constructor
    in the configuration files.

    Global dependencies can be added to the application settings under the ``piped_dependencies`` key, which
    should be a dict. This dict will be converted to a :class:`~piped.dependencies.DependencyMap` before being
    given to the :class:`cyclone.web.Application` instance as a setting.

    In order to enable debugging tracebacks from the web server, configure the cyclone application as in the example
    configuration above and make sure your request handlers subclasses :class:`piped_cyclone.handlers.DebuggableHandler`.

    .. warning:: Enabling debugging enables allowed clients to execute arbitrary code as the user running :program:`piped`.

    To make stack traces keep their frame information, use :option:`piped -D <piped -D>` when launching :program:`piped`.

    """
    interface.classProvides(resource.IResourceProvider)

    configuration_key = 'cyclone'

    def __init__(self):
        self._configured_handler_factory_ids = set()
        self._applications_by_name = dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager
        self.resource_manager = runtime_environment.resource_manager

        cm = runtime_environment.configuration_manager

        for name, config in cm.get(self.configuration_key, dict()).items():
            listen = str(config.pop('listen', '8888'))
            application_factory_type = config.pop('type', 'cyclone.web.Application')
            application_factory = reflect.namedAny(application_factory_type)

            application_config = config.pop('application', dict())

            handlers = application_config.pop('handlers', [])

            transforms = self._resolve_transforms(application_config.pop('transforms', list()))

            if 'ui_modules' in application_config:
                ui_modules = application_config['ui_modules']
                if isinstance(ui_modules, dict):
                    for key, value in ui_modules.items():
                        ui_modules[key] = reflect.namedAny(value)
                elif isinstance(ui_modules, basestring):
                    application_config['ui_modules'] = reflect.namedAny(ui_modules)
                else:
                    raise Exception('Unknown ui_modules type: {0}'.format(type(ui_modules)))

            for key, value in application_config.items():
                if key.endswith('_path') and isinstance(value, filepath.FilePath):
                    application_config[key] = value.path

            for i, handler in enumerate(handlers):
                handlers[i] = self._resolve_urlspec(name, handler)

            if 'piped_dependencies' in application_config:
                for key, value in application_config['piped_dependencies'].items():
                    application_config['piped_dependencies'][key] = dict(provider=value) if isinstance(value, basestring) else value
                application_config['piped_dependencies'] = self.dependency_manager.create_dependency_map(self, **application_config['piped_dependencies'])

            application = application_factory(handlers, transforms=transforms, **application_config)

            service = strports.service(listen, application)
            service.setServiceParent(runtime_environment.application)

            self._applications_by_name[name] = application

            self.resource_manager.register('cyclone.application.{0}'.format(name), provider=self)

    def add_consumer(self, resource_dependency):
        name = resource_dependency.provider.rsplit('.', 1)[-1]
        application = self._applications_by_name[name]
        resource_dependency.on_resource_ready(application)

    def _resolve_urlspec(self, site_name, url_spec):
        if isinstance(url_spec, (list, tuple)):
            url_spec = dict(zip(['pattern', 'handler', 'kwargs', 'name'], url_spec))

        if not 'pattern' in url_spec or not 'handler' in url_spec:
            msg = 'Missing attributes in handler configuration in site [{0}]: [{1}]'.format(site_name, url_spec)
            hint = 'Both "pattern" and "handler" must be defined.'
            raise InvalidHandlerError(msg, hint)

        # string -> class specification
        if isinstance(url_spec['handler'], basestring):
            url_spec['handler'] = {'class':url_spec['handler']}

        if 'provider' in url_spec['handler']:
            handler = url_spec.pop('handler')
            provider = handler['provider']
            dependency = self.dependency_manager.add_dependency(self, handler)
            url_spec.setdefault('kwargs', dict()).update(dependency=dependency)

            if provider.startswith('pipeline.'):
                url_spec['handler_class'] = handlers.PipelineRequestHandler
                return web.url(**url_spec)

            url_spec['handler_class'] = handlers.PipedRequestHandlerProxy(dependency)
            return web.url(**url_spec)

        if 'class' in url_spec['handler']:
            handler = url_spec.pop('handler')

            cls = reflect.namedAny(handler['class'])
            if id(cls) not in self._configured_handler_factory_ids:
                self._configured_handler_factory_ids.add(id(cls))

                try:
                    cls.configure(self.runtime_environment)
                except AttributeError as ae:
                    if "has no attribute 'configure'" not in ae.args[0]:
                        raise

            url_spec['handler_class'] = cls
            return web.url(**url_spec)

        msg = 'Invalid handler configuration in site [{0}]: [{1}]'.format(site_name, url_spec)
        raise InvalidHandlerError(msg)

    def _resolve_transforms(self, transforms):
        if not transforms:
            return None

        for i, transform in transforms:
            transforms[i] = reflect.namedAny(transform)

        return transforms
