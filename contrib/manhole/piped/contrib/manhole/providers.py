# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.application import internet, service
from twisted.conch import manhole, manhole_ssh, error as conch_error
from twisted.conch.insults import insults
from twisted.conch.ssh import keys
from twisted.cred import error, portal
from twisted.internet import defer
from twisted.python import reflect
from zope import interface

from piped import resource


class ManholeProvider(object, service.MultiService):
    """ Embeds manholes in Piped services.

    Configuration example::

        manholes:
            my_manhole:
                enabled: true # defaults to true
                port: 10022 # defaults to 10022
                keys:
                    public_key_file: path # or public_key: str
                    private_key_file: path # or private_key: str
                checkers: # multiple checkers are allowed
                    inmemory:
                        checker: twisted.cred.checkers.InMemoryUsernamePasswordDatabaseDontUse
                        arguments:
                            username: password
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)

    def configure(self, runtime_environment):
        self.setName('manhole')
        self.setServiceParent(runtime_environment.application)

        self.manholes = runtime_environment.get_configuration_value('manholes', dict())

        for manhole_name, manhole_configuration in self.manholes.items():
            if not manhole_configuration.get('enabled', True):
                continue # this manhole has been disabled, so don't create it
            manholeservice = ManholeService(manhole_configuration)
            manholeservice.setName(manhole_name)
            manholeservice.setServiceParent(self)
            manholeservice.configure(runtime_environment)


class PipedManhole(manhole.ColoredManhole):
    """ A colored manhole that handles a few extra key combinations. """
    def connectionMade(self):
        r = manhole.ColoredManhole.connectionMade(self)
        # add a keyhandler for what my macbook sends when Im hitting backspace
        self.keyHandlers['\x08'] = self.handle_BACKSPACE
        self.keyHandlers['\x01'] = self.handle_HOME # CTRL-A
        self.keyHandlers['\x05'] = self.handle_END # CTRL-E
        self.keyHandlers['\x15'] = self.handle_BACKSPACE_LINE # CTRL-U
        self.keyHandlers['\x17'] = self.handle_BACKSPACE_WORD # CTRL-W
        return r

    def handle_BACKSPACE_LINE(self):
        while self.lineBufferIndex > 0:
            self.handle_BACKSPACE()

    WORD_DELIMITERS = """ .;:({['\""""
    def handle_BACKSPACE_WORD(self):
        self.handle_BACKSPACE()
        while self.lineBufferIndex > 0 and self.lineBuffer[self.lineBufferIndex-1] not in self.WORD_DELIMITERS:
            self.handle_BACKSPACE()


class PipedConchFactory(manhole_ssh.ConchFactory):
    """ A conch factory that can be initialized with an explicit pair of
    public_key, private_key to use.
    """

    def __init__(self, portal, private_key=None, public_key=None, **kw):
        manhole_ssh.ConchFactory.__init__(self, portal)
        if private_key:
            self.privateKeys = {
                'ssh-rsa' : keys.Key.fromString(private_key)
            }

        if public_key:
            self.publicKeys = {
                'ssh-rsa' : keys.Key.fromString(public_key)
            }


class ManholeService(service.MultiService):
    """ A configurable manhole service.

    See ManholeProvider for a configuration example.
    """
    protocolFactory = PipedManhole
    conchFactory = PipedConchFactory

    def __init__(self, manhole_configuration):
        service.MultiService.__init__(self)
        self.manhole_configuration = manhole_configuration

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment

        self.key_config = self._normalize_key_config(self.manhole_configuration.get('keys', dict()))

        factory = self._make_factory()

        tcpservice = internet.TCPServer(self.manhole_configuration.get('port', 10022), factory)
        tcpservice.setName(self.name)
        tcpservice.setServiceParent(self)

        self._configure_dependencies(self.manhole_configuration.get('dependencies', dict()))

    def _configure_dependencies(self, dependency_map):
        for dependency_key, dependency_configuration in dependency_map.items():
            if isinstance(dependency_configuration, basestring):
                dependency_configuration = dependency_map[dependency_key] = dict(provider=dependency_configuration)

        self.dependencies = self.runtime_environment.create_dependency_map(self, **dependency_map)

    def _normalize_key_config(self, key_config):
        private_key_file = key_config.pop('private_key_file', None)
        public_key_file = key_config.pop('public_key_file', None)

        if private_key_file:
            private_key_file = getattr(private_key_file, 'path', private_key_file)
            key_config['private_key'] = open(private_key_file).read()

        if public_key_file:
            public_key_file = getattr(public_key_file, 'path', public_key_file)
            key_config['public_key'] = open(public_key_file).read()

        return key_config

    def _make_factory(self):
        checkers = self._make_checkers()
        realm = manhole_ssh.TerminalRealm()
        portal_ = MultipleCheckersPortal(realm, checkers)

        def chainProtocolFactory():
            return insults.ServerProtocol(self.protocolFactory, namespace=self._get_namespace())
        realm.chainedProtocolFactory = chainProtocolFactory

        factory = self.conchFactory(portal_, **self.key_config)

        return factory

    def _make_checkers(self):
        cs = list()
        for checker_config in self.manhole_configuration['checkers'].values():
            checker_name = checker_config.pop('checker')
            checker_factory = reflect.namedAny(checker_name)
            checker = checker_factory(**checker_config.get('arguments', dict()))
            cs.append(checker)
        return cs

    def _get_namespace(self):
        namespace = dict(runtime_environment=self.runtime_environment, dependencies=self.dependencies)
        for key, value in self.manhole_configuration.get('namespace', dict()).items():
            namespace[key] = reflect.namedAny(value)
        return namespace


class MultipleCheckersPortal(portal.Portal):
    """ A Portal subclass that authenticates against multiple checkers. """
    def registerChecker(self, checker, *credentialInterfaces):
        if not credentialInterfaces:
            credentialInterfaces = checker.credentialInterfaces
        for credentialInterface in credentialInterfaces:
            self.checkers.setdefault(credentialInterface, list()).append(checker)

    @defer.inlineCallbacks
    def login(self, credentials, mind, *interfaces):
        for i in self.checkers:
            if i.providedBy(credentials):
                for checker in self.checkers[i]:
                    try:
                        avatar_id = yield checker.requestAvatarId(credentials)
                        avatar = yield self.realm.requestAvatar(avatar_id, mind, *interfaces)
                        defer.returnValue(avatar)
                    except conch_error.ValidPublicKey:
                        # This is how SSHPublicKeyDatabase says "Your public key is okay, now prove you have
                        # the private key to continue".
                        raise
                    except error.UnauthorizedLogin:
                        continue
                raise error.UnauthorizedLogin()

        ifac = interface.providedBy(credentials)
        raise error.UnhandledCredentials("No checker for %s" % ', '.join(map(reflect.qual, ifac)))
