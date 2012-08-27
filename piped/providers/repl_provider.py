# Copyright (c) 2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import logging
import sys
import termios
import tempfile
import tty
import os

from twisted.conch import stdio as conch_stdio
from twisted.internet import reactor, stdio, fdesc
from zope import interface

from piped import resource
from twisted.python import reflect, filepath


logger = logging.getLogger(__name__)


class REPLProvider(object):
    """ Provides a local REPL (Read-Evaluate-Print-Loop), which acts as a Piped-enabled Python shell.

    Status: beta.

    There are currently two ways of starting the REPL:

        #. Using the piped command line switch ``--repl``. This may be used without any changes
           to your existing configuration, but only gives you access to the runtime environment
           and an empty dependency map by default.

        #. Setting ``repl.enabled`` to ``true`` in the configuration (see the example below)

    The REPL can be configured with a bootstrapping script and a dependency map. For example:

    .. code-block:: yaml

        repl:
            enabled: true # must be trueish for the REPL to start, may be set from the command line by using --repl
            repl_implementation: piped.providers.repl_provider.PipedREPL # (optional, default) override to change the repl implementation
            bootstrap: | #(optional, string or filepath)
                from twisted.web import client

                get = client.getPage
                post = lambda url, method='post', *a, **kw: client.getPage(url, method=method, *a, **kw)

            dependencies: #(optional)
                foo: context.foo

        contexts:
            foo:
                bar: baz

    Run piped:

    .. code-block:: bash

        $ piped -nc configuration.yaml
        ...
        >>> 1+2
        3
        >>> dir()
        ['_', '__builtins__', 'client', 'd', 'dependencies', 'get', 'post', 'runtime_environment']

    Use ``exit()`` or Ctrl-D (i.e. ``EOF``) to exit the REPL.
    """
    interface.classProvides(resource.IResourceProvider)

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager

        repl_configuration = runtime_environment.get_configuration_value('repl', dict())

        if not repl_configuration.pop('enabled', False):
            return

        repl_implementation_class = repl_configuration.pop('repl_implementation', reflect.fullyQualifiedName(PipedREPL))

        self.repl = reflect.namedAny(repl_implementation_class)(**repl_configuration)
        self.repl.configure(runtime_environment)


class PipedREPL(object):
    """ Default implementation of the REPL.

    Starts the REPL and hooks it up to stdin/stdout.
    """
    running = False

    def __init__(self, console_manhole=None, dependencies=None, bootstrap='', force_blocking_stdout=True):
        """
        :param console_manhole: Fully qualified name of the console manhole class to use.
            Defaults to :class:`piped.providers.repl_provider.PipedConsoleManhole`
        :param dependencies: A dict containing named dependencies
        :param bootstrap: A string containing the bootstrap script to use or a
            :class:`twisted.python.filepath.FilePath` instance that points to python file.
        :param force_blocking_stdout: Whether to make stdout blocking after creating the StandardIO interface.
            May be required on some platforms.
        """
        self.console_manhole_name = console_manhole or reflect.fullyQualifiedName(PipedConsoleManhole)
        self.console_manhole_factory = reflect.namedAny(self.console_manhole_name)

        self.dependencies = dependencies or dict()
        self.bootstrap = bootstrap

        self.force_blocking_stdout = force_blocking_stdout

        self.namespace = dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.namespace['runtime_environment'] = runtime_environment

        for name, dependency_configuration in self.dependencies.items():
            # if the configuration is a string, assume the string is a provider
            if isinstance(dependency_configuration, basestring):
                self.dependencies[name] = dict(provider=dependency_configuration)

        self.dependency_map = runtime_environment.create_dependency_map(self, **self.dependencies)

        # add the dependencies to the namespace
        self.namespace.setdefault('d', self.dependency_map)
        self.namespace.setdefault('dependencies', self.dependency_map)

        with tempfile.NamedTemporaryFile(suffix='_bootstrap.py') as ntf:
            try:
                if isinstance(self.bootstrap, filepath.FilePath):
                    execfile(self.bootstrap.path, self.namespace, self.namespace)
                else:
                    # run the bootstrap in the compiled namespace, using a tempfile to facilitate tracebacks
                    ntf.file.write(self.bootstrap)
                    ntf.file.flush()

                    execfile(ntf.name, self.namespace, self.namespace)
                    logger.info('Finished bootstrapping the REPL namespace.')
            except Exception as e:
                logger.error('Caught exception while bootstrapping REPL. Traceback follows.', exc_info=True)

        try:
            self.start()
            logger.info('REPL started.')
        except Exception as e:
            logger.error('Unable to start REPL. Traceback follows', exc_info=True)

    def start(self):
        self.__class__.running = True

        self.fd = sys.__stdin__.fileno()
        self.old_settings = termios.tcgetattr(self.fd)

        tty.setraw(self.fd)

        self.server_protocol = conch_stdio.ServerProtocol(self.console_manhole_factory, namespace=self.namespace)
        self.stdio = stdio.StandardIO(self.server_protocol)

        if self.force_blocking_stdout:
            fdesc.setBlocking(sys.__stdout__.fileno())

        # fix the terminal so the user does not have to reset it after using us
        reactor.addSystemEventTrigger('before', 'shutdown', self._stop)

    def _stop(self):
        termios.tcsetattr(self.fd, termios.TCSANOW, self.old_settings)
        os.write(self.fd, "\r\x1bc\r")
        self.__class__.running = False


class PipedConsoleManhole(conch_stdio.ConsoleManhole):
    """ A colored manhole that handles a few extra key combinations. """

    def initializeScreen(self):
        # don't reset the terminal at the beginning. TODO: don't reset when exiting either..
        #self.terminal.reset()
        self.setInsertMode()

    def connectionMade(self):
        r = conch_stdio.ConsoleManhole.connectionMade(self)
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

    def connectionLost(self, reason):
        if reactor.running:
            reactor.stop()