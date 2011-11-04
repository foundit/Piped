# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import random
import weakref

from zope import interface
from twisted.application import service, strports
from twisted.internet import error, defer, reactor, endpoints
from twisted.spread import pb
from twisted.cred import portal, credentials
from twisted.python import reflect

from piped import resource, event, log, exceptions, util


class PBClientProvider(object, service.MultiService):
    """ Provides twisted.spread.pb clients.

    For details on how perspective broker works, see the Perspective Broker section of the
    `Twisted Documentation <http://twistedmatrix.com/documents/current/core/howto/pb-intro.html>`.

    Example configuration::

        pb:
            clients:
                named_client:
                    endpoint: tcp:host=localhost:port=8789

                    # if the remote server requires a login, specify the username and password:
                    username: a_username
                    password: a_password

    Given the above configuration, two resources will be provided:

        * 'pb.client.named_client' which is a `PipedPBClientFactory`
        * 'pb.client.named_client.root_object', the root object (an
             `twisted.spread.pb.RemoteReference` instance.

    """
    interface.classProvides(resource.IResourceProvider)
    name = 'spread-client-provider'

    def __init__(self):
        service.MultiService.__init__(self)

        self._client_factories_by_name = dict()

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager
        resource_manager = runtime_environment.resource_manager

        self.resource_configurations = runtime_environment.get_configuration_value('pb.clients', dict())
        for resource_name, resource_config in self.resource_configurations.items():
            resource_manager.register('pb.client.%s'%resource_name, provider=self)
            resource_manager.register('pb.client.%s.root_object'%resource_name, provider=self)

    def add_consumer(self, resource_dependency):
        config = dict(zip(['pb', 'client', 'resource_name', 'kind'], resource_dependency.provider.split('.')))

        factory = self._get_or_create_client_factory(config['resource_name'])

        if config.get('kind') == 'root_object':
            factory.on_got_root_object +=  lambda factory, root_object: resource_dependency.on_resource_ready(root_object)
            factory.on_disconnected += lambda factory, reason: resource_dependency.on_resource_lost(reason)

            # if the dependency is added after the root object has been fetched:
            if factory._root and factory._broker.connected:
                resource_dependency.on_resource_ready(factory._root)
        else:
            factory.on_connected +=  lambda factory, broker: resource_dependency.on_resource_ready(factory)
            factory.on_disconnected += lambda factory, reason: resource_dependency.on_resource_lost(reason)

            # if the dependency is added after the connection has been established:
            if factory._broker and factory._broker.connected:
                resource_dependency.on_resource_ready(factory)

        # this isn't required, but creates a nicer looking dependency graph
        self.dependency_manager.add_dependency(resource_dependency, factory)
    
    def _get_or_create_client_factory(self, resource_name):
        if not resource_name in self._client_factories_by_name:
            factory = PipedPBClientFactory(**self.resource_configurations[resource_name])

            factory.setServiceParent(self)

            self._client_factories_by_name[resource_name] = factory

        return self._client_factories_by_name[resource_name]


class PBServerProvider(object, service.MultiService):
    """ Provides batons from a twisted.spread.pb server.

    For details on how perspective broker works, see the Perspective Broker section of the
    `Twisted Documentation <http://twistedmatrix.com/documents/current/core/howto/pb-intro.html>`.

    Example configuration::

        pb:
            servers:
                named_server:
                    listen: tcp:8789
                    processor: processor_name
                    wait_for_processor: False # this is the default

                    # if the processor does not callback the deferred, the following default is used
                    # if the default is not provided, the deferred will be errbacked.
                    default_callback: None

                    # if a checker is specified, access to the processor will be restricted,
                    # and the baton will contain the avatar_id.
                    checker:
                        name: twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse
                        arguments:
                            username: password

    Given the above configuration, a pb.PBServerFactory will be created to listen on the
    specified port. When methods are called on the server, a baton will be produced. If
    ``wait_for_processor`` is ``True``, incoming batons are buffered until the processor
    becomes available, otherwise the client receives an errback immediately.

    The baton contains the following keys:

    message
        The name of the function being called

    args
        list of arguments

    kwargs
        dict of keyword arguments

    avatar_id
        The username of the authenticated user. This key is only set if a checker was configured.

    deferred
        A `twisted.internet.defer.Deferred` object that the processor should
        callback or errback in order to produce a response to the client.

    """
    interface.classProvides(resource.IResourceProvider)
    name = 'spread-server-provider'

    def __init__(self):
        service.MultiService.__init__(self)

        self._servers_by_name = dict()

    def configure(self, runtime_environment):
        self.setName('PB server provider')
        self.setServiceParent(runtime_environment.application)
        
        self.runtime_environment = runtime_environment

        self.resource_configurations = runtime_environment.get_configuration_value('pb.servers', dict())
        
        for resource_name, resource_config in self.resource_configurations.items():
            runtime_environment.resource_manager.register('pb.server.%s'%resource_name, provider=self)

            server = PipedPBService(**resource_config)
            server.configure(runtime_environment)

            self._servers_by_name[resource_name] = server

            server.setServiceParent(self)

    def add_consumer(self, resource_dependency):
        pb, server, resource_name = resource_dependency.provider.split('.')

        server = self._servers_by_name[resource_name]

        resource_dependency.on_resource_ready(server)

        self.runtime_environment.dependency_manager.add_dependency(resource_dependency, server)


class PipedPBServerFactory(pb.PBServerFactory):
    """ A PBServerFactory that stores a set of connected client protocols. """
    protocols = set()

    def buildProtocol(self, addr):
        protocol = pb.PBServerFactory.buildProtocol(self, addr)

        # we store the protocols in order to be able to aggressively drop their connections
        # if we stop the service
        self.protocols.add(protocol)
        protocol.notifyOnDisconnect(lambda: self.protocols.remove(protocol))
        
        return protocol


class PipedPBRoot(pb.Root):
    """ An unsecured PB root object. """

    def __init__(self, server):
        self.server = server
    
    def remoteMessageReceived(self, broker, message, args, kwargs):
        """ A remote message has been received.  Dispatch it appropriately. """
        args = broker.unserialize(args)
        kwargs = broker.unserialize(kwargs)

        baton = dict(message=message, args=args, kwargs=kwargs)

        d = self.server.handle_baton(baton)

        return broker.serialize(d, self.perspective)



class PipedUserPerspective(pb.Avatar):
    """ An authenticated clients perspective. """
    
    def __init__(self, avatar_id, server):
        self.avatar_id = avatar_id
        self.server = server

    def perspectiveMessageReceived(self, broker, message, args, kwargs):
        """ A remote message has been received.  Dispatch it appropriately. """
        args = broker.unserialize(args, self)
        kwargs = broker.unserialize(kwargs, self)
        
        baton = dict(message=message, args=args, kwargs=kwargs, avatar_id=self.avatar_id)

        d = self.server.handle_baton(baton)

        return broker.serialize(d, self)


class PipedPBRealm(object):
    interface.implements(portal.IRealm)

    def __init__(self, server):
        self.server = server

    def requestAvatar(self, avatarId, mind, *interfaces):
        """ Returns an avatar for an authenticated user. """
        if not pb.IPerspective in interfaces:
            raise NotImplementedError()

        return pb.IPerspective, PipedUserPerspective(avatar_id=avatarId, server=self.server), lambda: None


class PipedPBService(pb.Root, service.MultiService):
    """ A perspective broker service for Piped. """
    
    def __init__(self, listen, processor, wait_for_processor=False, default_callback=Ellipsis, checker=None):
        service.MultiService.__init__(self)
        
        if isinstance(listen, basestring):
            listen = [listen]
        self.listen = listen

        self.processor_config = dict(provider=processor) if isinstance(processor, basestring) else processor
        self.wait_for_processor = wait_for_processor
        self.default_callback = default_callback

        self.checker = checker

    def configure(self, runtime_environment):
        dependency_manager = runtime_environment.dependency_manager
        self.processor_dependency = dependency_manager.add_dependency(self, self.processor_config)

        root = PipedPBRoot(server=self)

        if self.checker:
            Checker = reflect.namedAny(self.checker['name'])
            self.checker = Checker(**self.checker.get('arguments', dict()))

            self.realm = PipedPBRealm(server=self)

            root = portal.Portal(self.realm)
            root.registerChecker(self.checker)

        self.factory = PipedPBServerFactory(root)

        for listen in self.listen:
            service = strports.service(listen, self.factory)
            service.setServiceParent(self)

    @defer.inlineCallbacks
    def stopService(self):
        # we avoid stopping child services that are not running and have a _waitingForPort because
        # of our workaround that enables stopped endpoint services to be restarted:
        ds = list()
        for serv in self.services:
            if not serv.running and hasattr(serv, '_waitingForPort'):
                continue
            ds.append(serv.stopService())

        yield defer.DeferredList(ds, consumeErrors=True)

        # aggressively close all active connections
        for protocol in self.factory.protocols:
            protocol.transport.loseConnection()
        
        # this is a workaround that enables stopped endpoint services to be restarted, since
        # they will have to request their port again in order to start properly.
        for serv in self.services:
            if not serv.running and hasattr(serv, '_waitingForPort'):
                if serv._waitingForPort and serv._waitingForPort.called:
                    serv._waitingForPort = None

    @defer.inlineCallbacks
    def handle_baton(self, baton):
        """ Processes the baton using the processor. """

        if self.wait_for_processor:
            processor = yield self.processor_dependency.wait_for_resource()
        else:
            # if the processor is unavailable, this will raise an exception that
            # is propagated back to the client.
            processor = self.processor_dependency.get_resource()

        deferred = baton['deferred'] = defer.Deferred()

        # from here now, however, the processor is in charge of callbacking or errbacking
        # the deferred.
        yield processor(baton)

        # we check if we have a result here, because we want to avoid having to monitor
        # the garbage collection of the deferred unless required.
        if deferred.called:
            result = yield deferred
            defer.returnValue(result)

        # we want to ensure that the client gets an response, so we add an callback that will
        # be called when the deferred we provided in the baton are garbage collected. when it
        # is finalized, we make sure that the client has gotten a response

        deferred = defer.Deferred()

        baton['deferred'].addCallback(deferred.callback)
        baton['deferred'].addErrback(deferred.errback)

        ref = weakref.ref(baton['deferred'], lambda ref: self._handle_deferred_gc(ref, deferred))

        # delete our reference to the baton since we'll be asynchronously waiting for the deferred
        # in the baton to be callbacked or finalized, and we do not want us having a reference
        # to the deferred influence the garbage collection
        del baton
        # since we're in a generator, the inlineCallbacks decorator might have a reference to our
        # gi_frame.f_locals, which needs to be updated in order for the above del statement to take effect:
        locals()

        result = yield deferred
        defer.returnValue(result)

    def _handle_deferred_gc(self, ref, deferred):
        """ This function is called when the deferred in the baton have been finalized. """
        # if the client haven't gotten an answer, provide a default
        if not deferred.called:
            if self.default_callback is not Ellipsis:
                deferred.callback(self.default_callback)
            else:
                e_msg = 'The processor did not callback/errback the deferred.'
                detail = 'Additionally, no default_callback was configured.'
                deferred.errback(exceptions.MissingCallback(e_msg, detail))


class RemoteError(pb.Error):
    """
    When the remote side of the connection returns an exception back, it comes
    in as a string. Raising string exceptions doesn't work in Python 2.6 or
    greater, so this exception is used instead.
    """
    def __init__(self, type, value):
        super(Exception, self).__init__(type, value)
        self.remoteType = type
        self.value = value


class PipedCopiedFailure(pb.CopiedFailure):
    """ A workaround for `twisted.spread.pb.CopiedFailure` being impossible to
    use with inlineCallbacks because the exceptions may have been turned into
    strings.
    """
    def throwExceptionIntoGenerator(self, g):
        # if we have a string exception, wrap it in a RemoteError, since
        # python 2.6 is strict about not having string exceptions
        if isinstance(self.type, (str, unicode)):
            return g.throw(RemoteError, (self.type, self.value), self.tb)
        return g.throw(self.type, self.value, self.tb)

# This overrides the unjellying for the pb.CopyableFailure, making sure our workaround gets used.
pb.setUnjellyableForClass(pb.CopyableFailure, PipedCopiedFailure)


class PipedPBClientFactory(pb.PBClientFactory, service.Service):
    """ A reconnecting factory. """
    max_delay = 30
    initial_delay = 1.0
    # Note: These highly sensitive factors have been precisely measured by
    # the National Institute of Science and Technology.  Take extreme care
    # in altering them, or you may damage your Internet!
    # (Seriously: <http://physics.nist.gov/cuu/Constants/index.html>)
    factor = 2.7182818284590451 # (math.e)
    # Phi = 1.6180339887498948 # (Phi is acceptable for use as a
    # factor if e is too large for your application.)
    jitter = 0.11962656472 # molar Planck constant times c, joule meter/mole

    delay = initial_delay
    retries = 0

    max_retries = None

    _connecting = None
    
    def __init__(self, endpoint, username=None, password=None):
        pb.PBClientFactory.__init__(self)
        
        self.endpoint = endpoint
        self.username = username
        self.password = password
        
        self.on_connected = event.Event()
        self.on_got_root_object = event.Event()
        self.on_failed_getting_root_object = event.Event()
        self.on_disconnected = event.Event()

    @defer.inlineCallbacks
    def keep_connecting(self):
        self.retries = 0
        self.delay = self.initial_delay

        while self.running:
            try:
                yield self.client.connect(self)
            except error.ConnectError as e:
                pass
            else:
                self._connecting = None
                break

            self.retries += 1
            if self.max_retries is not None and (self.retries > self.max_retries):
                log.info("Abandoning %s after %d retries." %(self.client, self.retries))
                break

            self.delay = min(self.delay * self.factor, self.max_delay)
            if self.jitter:
                self.delay = random.normalvariate(self.delay, self.delay * self.jitter)

            log.info("%s will retry in %d seconds" % (self.client, self.delay,))
            yield util.wait(self.delay)

    def startService(self):
        if not self.running:
            self.running = True

            self.client = endpoints.clientFromString(reactor, self.endpoint)

            if not self._connecting:
                self._connecting = self.keep_connecting()

        service.Service.startService(self)

    def stopService(self):
        if self.running:
            self.disconnect()
        
        service.Service.stopService(self)

    def _consider_retrying(self):
        # errback all pending getRootObject requests
        self._failAll(None)
        # TODO: specify a reason why we disconnected
        self.on_disconnected(self, None)

        if self.running and not self._connecting:
            self._connecting = self.keep_connecting()

    def clientConnectionMade(self, broker):
        broker.notifyOnDisconnect(self._consider_retrying)
        pb.PBClientFactory.clientConnectionMade(self, broker)

        self.on_connected(self, broker)

        if self.username:
            creds = credentials.UsernamePassword(self.username, self.password)
            d = self.login(creds)
            d.addCallback(lambda perspective: self.on_got_root_object(self, perspective))
            d.addErrback(self._log_error_getting_root_object)
        else:
            d = self.getRootObject()
            d.addCallback(lambda root_object: self.on_got_root_object(self, root_object))
            d.addErrback(self._log_error_getting_root_object)

    def _log_error_getting_root_object(self, reason):
        self.on_failed_getting_root_object(reason)
        log.error(reason)
        
