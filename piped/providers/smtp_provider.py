# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from email import parser
from StringIO import StringIO

from zope import interface
from twisted.application import service, strports
from twisted.cred import portal
from twisted.python import reflect
from twisted.internet import defer, reactor
from twisted.mail import smtp, imap4
from twisted.internet.interfaces import ISSLTransport

from piped import resource
from piped.processing import util


def send_mail(from_addr, to_addr, file, host='localhost', port=25, timeout=60, retries=5,
             use_ssl=False, require_transport_security=None,
             require_authentication=None, username=None, password=None,
             helo_fallback=False):
    """ A utility function used to send email.

    :param from_addr: Sender email address.
    :param to_addr: Recipients email address.
    :param file: The full message, including headers. Any file-like or any object with
        a ``__str__`` can be used.

    :param host: Which SMTP server to use.
    :param port: Which port to connect to.

    :param use_ssl: Whether the server speaks SSL.
    :param require_transport_security: Whether to require transport security (TLS) before
        sending the email.

        Defaults to ``True`` if a username is specified.

        Defaults to ``False`` if using SSL due to `Twisted #3989
        <http://twistedmatrix.com/trac/ticket/3989>`_.

    :param require_authentication: Whether to login before sending an email.
        Defaults to ``True`` if a username is specified, ``False`` otherwise.
    :param username: The username used when logging in.
    :param password: The password used when logging in.

    :param timeout: Number of seconds to wait before timing out a connection.  If
        ``None``, perform no timeout checking.
    :param retries: Number of retries if we cannot connect to the server.

    :param helo_fallback: Whether to fallback to HELO if EHLO fails.

    """

    if hasattr(file, 'as_string'):
        # this looks like a email.message.Message
        file = StringIO(file.as_string())

    if not hasattr(file, 'read'):
        file = StringIO(str(file))

    if require_transport_security is None:
        if use_ssl:
            require_transport_security = False
        else:
            require_transport_security = username is not None

    if require_authentication is None:
        require_authentication = username is not None

    context_factory = None
    if use_ssl or require_transport_security:
        # only import the ssl module if required
        from twisted.internet import ssl
        context_factory = ssl.ClientContextFactory()

    deferred = defer.Deferred()

    sender_factory = smtp.ESMTPSenderFactory(
        contextFactory = context_factory,
        deferred = deferred,
        timeout = timeout,
        fromEmail = from_addr,
        toEmail = to_addr,
        file = file,
        retries = retries,
        requireTransportSecurity = require_transport_security,
        requireAuthentication = require_authentication,
        username = username,
        password = password,
        heloFallback = helo_fallback,
    )

    if use_ssl:
        reactor.connectSSL(host, port, sender_factory, context_factory)
    else:
        reactor.connectTCP(host, port, sender_factory)

    return deferred


class SMTPProvider(object, service.MultiService):
    """ Provides an SMTP interface.

    This provider uses ``strports`` to specify listening ports. For more information, see
    `strports in the Twisted documentation
    <http://twistedmatrix.com/documents/11.0.0/api/twisted.internet.endpoints.html#serverFromString>`_.

    Example configuration:

    .. code-block:: yaml

        smtp:
            my_server:
                # a strport or a list of strports to listen to
                listen: tcp:10025

                # the processor that will handle the messages
                processor: processor_name

                # to enable STARTTLS, specify the following:
                tls:
                    private_key: examples/server.key
                    certificate: examples/server.crt

                # The following header is added to the email when it is received.
                # if this returns null, no header is added.
                # According to RFC 821, the Received header should be in the following form:
                #   Received: FROM domain BY domain [VIA link] [WITH protocol] [ID id] [FOR path];

                received_header: 'protocol, helo, origin, recipients: "Received: BY piped.localhost WITH SMTP"'

                # The namespaced used by the validators
                namespace:
                    path: os.path

                # Validate the to and from addresses before the mail is accepted:
                validate_to: 'proto, user: True if str(user.dest) == "accepted@sender.com" else False'
                    # user is a twisted.mail.smtp.User instance

                validate_from: 'proto, helo, origin: True if str(origin) == "accepted@recipient.com" else False'
                    # helo is a tuple of (helo_string, client_ip)
                    # origin is a twisted.mail.smtp.Address instance

                # create a checker for authenticating the sender. if a checker is
                # set, AUTH will be required before any mail is accepted. the server
                # will not accept plaintext auth over an unencrypted connection (use
                # either ssl or STARTTLS).
                checker:
                    name: twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse
                    arguments:
                        username: password


    The processor will be invoked with a :class:`dict`\s containing the following keys:

    message
        An :class:`email.message.Message` instance.

    from_addr
        The senders email address.

    to_addr
        The recipients email address

    avatar_id
        The username of the authenticated user. This key is only set if a checker was configured.
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)

        self.smtp_configs = runtime_environment.get_configuration_value('smtp', dict())

        for server_name, server_config in self.smtp_configs.items():
            service = PipedSMTPServer(**server_config)
            service.configure(runtime_environment)
            service.setServiceParent(self)


class ProcessorMessage(object):
    interface.implements(smtp.IMessage)

    avatar_id = Ellipsis

    def __init__(self, server, from_addr, to_addr):
        self.parser = parser.FeedParser()
        self.processor_dependency = server.processor_dependency

        self.from_addr = from_addr
        self.to_addr = to_addr

    def lineReceived(self, line):
        self.parser.feed(line+'\n')

    @defer.inlineCallbacks
    def eomReceived(self):
        message = self.parser.close()
        self.parser = parser.FeedParser()

        processor = yield self.processor_dependency.wait_for_resource()

        baton = dict(message=message, from_addr=self.from_addr, to_addr=self.to_addr)
        # if we have an avatar_id, add it to the baton before processing:
        if self.avatar_id is not Ellipsis:
            baton['avatar_id'] = self.avatar_id

        yield processor(baton)

        defer.returnValue(None)


class PipedESMTP(smtp.ESMTP):
    """ An ESMTP protocol implementation.

    :ivar delivery: An :class:`twisted.mail.smtp.IMessageDelivery` that is responsible for
        validating and delivering email messages for this instance. This is usually set by
        the portal and contains the ``avatar_id`` instance variable.

    :ivar portal: A :class:`twisted.cred.portal.Portal` instance that is responsible for
        authenticating users when logging in.

    :ivar avatar_id: The username of the authenticated user.
    """
    interface.implements(smtp.IMessageDelivery)

    avatar_id = None
    from_addr = None
    server = None

    def __init__(self, *a, **kw):
        smtp.ESMTP.__init__(self, *a, **kw)

        self.notifiers = set()

    def ext_AUTH(self, rest):
        # if we aren't encrypted, do not accept any authentication
        if ISSLTransport.providedBy(self.transport):
            return smtp.ESMTP.ext_AUTH(self, rest)

        self.sendCode(530, 'Encrypt the connection by using SSL or STARTTLS before authenticating.')

    @defer.inlineCallbacks
    def validateFrom(self, helo, origin):
        self.from_addr = str(origin)
        
        if self.delivery:
            result = yield self.delivery.validateFrom(helo, origin)
            defer.returnValue(result)
        elif self.portal:
            # we have a portal, but no configured delivery, so we try to let our superclass
            # perform an anonymous login, which will set our delivery and retry this method
            
            result = yield smtp.ESMTP.validateFrom(self, helo, origin)
            defer.returnValue(result)

        validated = yield self.server.validate_from(self, helo, origin)

        if validated:
            defer.returnValue(origin)
        raise smtp.SMTPBadSender(origin)

    @defer.inlineCallbacks
    def validateTo(self, user):
        
        validated = yield self.server.validate_to(self, user)
        if validated:
            message = ProcessorMessage(self.server, self.from_addr, to_addr=str(user.dest))

            # if the delivery exists, we use its avatar_id
            if self.delivery:
                message.avatar_id = self.delivery.avatar_id

            defer.returnValue(lambda: message)
        
        raise smtp.SMTPBadRcpt(user.dest)

    def receivedHeader(self, helo, origin, recipients):
        return self.server.received_header(self, helo, origin, recipients)

    def notifyOnDisconnect(self, notify):
        self.notifiers.add(notify)

    def connectionLost(self, reason):
        smtp.ESMTP.connectionLost(self, reason)

        for notify in self.notifiers:
            notify()


class PipedSMTPRealm(object):
    interface.implements(portal.IRealm)

    def __init__(self, server):
        self.server = server

    def requestAvatar(self, avatarId, mind, *interfaces):
        # if we are authenticating a IMessageDelivery
        if smtp.IMessageDelivery in interfaces:

            delivery = PipedESMTP()
            delivery.avatar_id = avatarId
            self.server.prepare_protocol(delivery)

            # a tuple of the implemented interface, an instance implementing it and a logout callable
            return smtp.IMessageDelivery, delivery, lambda: None
        
        raise NotImplementedError()


class PipedSMTPServerFactory(smtp.SMTPFactory):
    protocol = PipedESMTP

    def __init__(self, server, *a, **kw):
        smtp.SMTPFactory.__init__(self, *a, **kw)
        self.server = server

    def buildProtocol(self, addr):
        p = smtp.SMTPFactory.buildProtocol(self, addr)
        self.server.prepare_protocol(p)
        return p


class PipedSMTPServer(service.MultiService):

    def __init__(self, listen, processor, namespace=None,
                 validate_to='protocol, user: False',
                 validate_from='protocol, helo, origin: False',
                 received_header='protocol, helo, origin, recipients: ' + \
                    '"Received: from %s by %s with ESMTP"%(protocol.transport.getPeer().host, protocol.host)',
                 checker=None,
                 tls = None):
        service.MultiService.__init__(self)

        if isinstance(listen, basestring):
            listen = [listen]
        self.listen = listen

        if isinstance(checker, basestring):
            checker = dict(name=checker)
        self.checker = checker

        self.tls = tls
        self.context_factory = None

        self.namespace = namespace or dict()
        self.validate_to = util.create_lambda_function(validate_to, **self.namespace)
        self.validate_from = util.create_lambda_function(validate_from, **self.namespace)
        self.received_header = util.create_lambda_function(received_header, **self.namespace)

        self.processor_config = dict(provider=processor) if isinstance(processor, basestring) else processor

        self.protocols = set()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

        dependency_manager = runtime_environment.dependency_manager
        self.processor_dependency = dependency_manager.add_dependency(self, self.processor_config)

        smtp_portal = None
        if self.checker:
            Checker = reflect.namedAny(self.checker['name'])
            checker = Checker(**self.checker.get('arguments', dict()))

            smtp_portal = portal.Portal(PipedSMTPRealm(server=self))
            smtp_portal.registerChecker(checker)

        if self.tls:
            from twisted.internet import ssl
            self.context_factory = ssl.DefaultOpenSSLContextFactory(
                privateKeyFileName=util.expand_filepath(self.tls['private_key']),
                certificateFileName=util.expand_filepath(self.tls['certificate'])
            )

        self.server_factory = PipedSMTPServerFactory(self, portal=smtp_portal)

        for listen in self.listen:
            server = strports.service(listen, self.server_factory)
            server.setServiceParent(self)

    def prepare_protocol(self, protocol):
        self.protocols.add(protocol)
        protocol.notifyOnDisconnect(lambda: self.protocols.remove(protocol))

        protocol.server = self

        # we only support LOGIN and PLAIN for now, and require an encrypted
        # connection when performing the authentication.
        if self.checker:
            protocol.challengers = dict(
                LOGIN=imap4.LOGINCredentials,
                PLAIN=imap4.PLAINCredentials
            )

        if self.context_factory:
            # the ESMTP protocol requires a ctx which is a ContextFactory
            # instance in order to perform a STARTTLS
            protocol.ctx = self.context_factory
        
        return protocol

    @defer.inlineCallbacks
    def stopService(self):
        yield service.MultiService.stopService(self)

        for protocol in self.protocols:
            if protocol.transport:
                protocol.transport.loseConnection()