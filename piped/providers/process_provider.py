# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import os

from twisted.application import service
from twisted.internet import defer, reactor, protocol, error
from twisted.protocols import basic
from twisted.python import reflect, failure
from zope import interface

from piped import event, exceptions, log, util, resource


class ProcessProvider(object, service.Service):
    """ Provides processes.

    Example configuration:

        processes:
            tail:
                executable: tail
                args: ['-f', 'somefile.txt']
            python:
                protocol: piped.providers.process_provider.DefaultProcessProtocol
                enabled: true
                spawn: true # set to false if you want to manually spawn this process using .spawn_process()
                spawn_consumerless: false # set to true to spawn the process even if it has no conumers
                executable: /usr/bin/python
                path: /usr/local
                restart: true # whether to restart the process when it exits.
                restart_wait: 1 # number of seconds to wait before restarting the process.
                args: ['-u', '-i'] # set the interpreter to be unbuffered and interactive
                stdout: # these are the defaults
                    protocol: piped.providers.process_provider.DefaultOutput
                    delimiter: '\n'
                    max_length: 16384
                stderr:
                    protocol: piped.providers.process_provider.RedirectToStdout
                stdin:
                    protocol: piped.providers.process_provider.DefaultInput

    Given the above configuration, this resource provider will provide the following
    resource paths:

        process.tail.stdout, process.tail.stderr, process.tail.stdin
        process.python.stdout, process.python.stderr, process.python.stdin

    When using the default protocols, the .stdout and .stderr interfaces conform to the
    resource.ISource interface.
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        self.on_start = event.Event()
        self.on_pause = event.Event()

        self._process_protocols_by_process_name = dict()
        self._spawn_process_protocols = set() # processes in this set are spawned at start_providing

    @property
    def process_configurations(self):
        return self.runtime_environment.get_configuration_value('processes', dict())

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager
        self.resource_manager = runtime_environment.resource_manager

        for process_name, process_config in self.process_configurations.items():
            # using pop because we pass the process_config to the process protocols init function.
            if not process_config.pop('enabled', True):
                # do not provide disabled processes
                continue

            self._provide_process(process_name, process_config)

        self.setServiceParent(runtime_environment.application)

    def _provide_process(self, process_name, process_config):
        process_kwargs = process_config.copy()

        spawn_consumerless = process_kwargs.pop('spawn_consumerless', True)
        spawn = process_kwargs.pop('spawn', True)

        process_protocol = self._make_and_configure_process_protocol(process_name, process_kwargs)

        if spawn and spawn_consumerless:
            # we're asked to spawn this one even without consumers. so add it to the set
            # of processes that will be spawned when we start providing.
            self._spawn_when_running(process_protocol)

        self.resource_manager.register('process.%s'%process_name, provider=self)
        for resource_name in process_protocol.get_resource_names():
            self.resource_manager.register('process.%s.%s'%(process_name,resource_name), provider=self)

    def _make_and_configure_process_protocol(self, process_name, process_config):
        protocol_name = process_config.pop('protocol', 'piped.providers.process_provider.DefaultProcessProtocol')
        process_protocol = IPipedProcessProtocol(reflect.namedAny(protocol_name)(self, process_name, **process_config))
        process_protocol.configure(self.runtime_environment)
        self._process_protocols_by_process_name[process_name] = process_protocol
        return process_protocol

    def add_consumer(self, resource_dependency):
        provider = resource_dependency.provider.split('.')
        (process, process_name), resource_name = provider[:2], provider[2:]

        process_protocol = self._get_named_process(process_name)

        # spawn the process if required
        if self.process_configurations[process_name].get('spawn', True):
            self._spawn_when_running(process_protocol)

        # we default to providing the process_protocol
        resource = process_protocol
        if resource_name:
            # if we have a resource_name, we provide that resource to the consumer
            resource = process_protocol.get_resource(resource_name[0])

        # TODO: pausing of processes isn't really supported
        # TODO: use a more specific exception class.
        pause_exception = exceptions.PipedError
        pause_msg = 'Process paused.'

        self.on_start += lambda: resource_dependency.on_resource_ready(resource)
        self.on_pause += lambda: resource_dependency.on_resource_lost(failure.Failure(pause_exception(pause_msg)))

        # make the dependency manager produce nicer graphs:
        self.dependency_manager.add_dependency(resource_dependency, resource)

        # the process might already be running
        if self.running:
            resource_dependency.on_resource_ready(resource)

    def _spawn_when_running(self, process_protocol):
        if process_protocol.process:
            # the process is already spawned
            return

        if self.running:
            # we've already started, so the process can be started immediately
            process_protocol.spawn_process()
            return

        # otherwise, defer the spawning until our service is running
        self._spawn_process_protocols.add(process_protocol)

    def _get_named_process(self, process_name):
        return self._process_protocols_by_process_name[process_name]

    def startService(self):
        service.Service.startService(self)
        self.on_start()
        for process_protocol in self._spawn_process_protocols:
            process_protocol.spawn_process()
        self._spawn_process_protocols = set()

    def stopService(self):
        service.Service.stopService(self)
        self.on_pause()


class IPipedProcessProtocol(interface.Interface):
    """ An interface that manages a process. """
    def __init__(self, process_provider, process_name, **process_config):
        pass

    def configure(self, runtime_environment):
        """ Configure the protocol. """

    def spawn_process(self):
        """ Spawn the process. """

    def get_resource_names(self):
        """ Return a list of resources provided by this process protocol. """

    def get_resource(self, resource_name):
        """ Return a named resource.

        This will be called before spawn_process whenever a dependency is resolved.
        """


class IProcessOutputProtocol(interface.Interface):
    """ An interface for protocols that parses output from a process. """

    def __init__(self, process_protocol, type, **extra_output_config):
        """ The protocol will be initialized with the parent process_protocol,
        the output type and any additional options specified in the configuration. """

    def configure(self, runtime_environment):
        """ Configure the protocol. """

    def connectionMade(self):
        """ Indicates that the connection has been made. """

    def dataReceived(self, data):
        """ Called when data has arrived from the process. """

    def processEnded(self, reason):
        """ Indicates that the process has ended. """


class IProcessInputProtocol(interface.Interface):
    """ An interface for protocols that can write data to a process. """

    def __init__(self, process_protocol, **extra_input_config):
        """ The protocol will be initialized with the parent process_protocol,
        and a any additional options specified in the configuration. """

    def configure(self, runtime_environment):
        """ Configure the protocol. """

    def write(self, data):
        """ Write some data. """


class DefaultProcessProtocol(protocol.ProcessProtocol):
    """ A default process protocol implementation.

    In order to understand how this class works, reading the first five sections of
    http://twistedmatrix.com/documents/current/core/howto/process.html is highly
    recommended.

    This class provides three resources: stdout, stderr, stdin. Each of these may
    have their own pluggable protocols. The former two uses the IProcessOutputProtocol
    interface while the latter uses the IProcessInputProtocol interface.
    """
    interface.implements(IPipedProcessProtocol)

    process = None

    default_output_protocol_name = 'piped.providers.process_provider.DefaultOutput'
    default_input_protocol_name = 'piped.providers.process_provider.DefaultInput'

    def __init__(self, provider, process_name, executable, restart=False, restart_wait=1, args=None, path=None, stdout=None, stderr=None, stdin=None):
        self.process_name = process_name
        self.executable = executable
        self.restart = restart
        self.restart_wait = restart_wait
        self.args = args or list()

        self.path = None
        if path:
            self.path = util.expand_filepath(path)

        stdout_config = stdout or dict()
        stdout_protocol_name = stdout_config.pop('protocol', self.default_output_protocol_name)

        stderr_config = stderr or dict()
        stderr_protocol_name = stderr_config.pop('protocol', self.default_output_protocol_name)

        stdin_config = stdin or dict()
        stdin_protocol_name = stdin_config.pop('protocol', self.default_input_protocol_name)

        self.stdout_protocol = IProcessOutputProtocol(reflect.namedAny(stdout_protocol_name)(self, 'stdout', **stdout_config))
        self.stderr_protocol = IProcessOutputProtocol(reflect.namedAny(stderr_protocol_name)(self, 'stderr', **stderr_config))
        self.stdin_protocol = IProcessInputProtocol(reflect.namedAny(stdin_protocol_name)(self, **stdin_config))

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        dependency_manager = runtime_environment.dependency_manager

        for protocol in self.stdout_protocol, self.stderr_protocol, self.stdin_protocol:
            protocol.configure(runtime_environment)
            dependency_manager.add_dependency(self, protocol)

        reactor.addSystemEventTrigger('before', 'shutdown', setattr, self, 'restart', False)

    def spawn_process(self):
        args = [self.executable]+self.args
        self.process = reactor.spawnProcess(self, self.executable, args=args, env=os.environ, path=self.path)
        reactor.addSystemEventTrigger('before', 'shutdown', self._signal_process, self.process, 'TERM')
        reactor.addSystemEventTrigger('during', 'shutdown', self._signal_process, self.process, 'KILL')

    def _signal_process(self, process, signal):
        try:
            process.signalProcess(signal)
        except error.ProcessExitedAlready as e:
            pass

    def get_resource_names(self):
        return 'stdout', 'stderr', 'stdin'

    def get_resource(self, resource_name):
        if resource_name == 'stdout':
            return self.stdout_protocol
        elif resource_name == 'stderr':
            return self.stderr_protocol
        elif resource_name == 'stdin':
            return self.stdin_protocol

    def connectionMade(self):
        log.info('Process %s has started.'%self.process_name)
        for protocol in self.stdout_protocol, self.stderr_protocol, self.stdin_protocol:
            protocol.connectionMade()

    def processEnded(self, reason):
        if reason.value == error.ProcessDone:
            log.info('Process %s terminated normally.'%self.process_name)
        else:
            log.warn('Process %s terminated with exit code %s.'%(self.process_name, reason.value.exitCode))

        for protocol in self.stdout_protocol, self.stderr_protocol, self.stdin_protocol:
            protocol.processEnded(reason)

        if self.restart:
            reactor.callLater(self.restart_wait, self.spawn_process)

    def outReceived(self, data):
        self.stdout_protocol.dataReceived(data)

    def errReceived(self, data):
        self.stderr_protocol.dataReceived(data)


class DefaultInput(object):
    interface.implements(IProcessInputProtocol)
    def __init__(self, process_protocol):
        self.process_protocol = process_protocol

    def configure(self, runtime_environment):
        pass

    def write(self, data):
        self.process_protocol.transport.write(data)

    def connectionMade(self):
        pass

    def processEnded(self, reason):
        pass


class EchoToStdout(DefaultInput):
    """ Echoes input to process_protocol.outReceived. """
    def write(self, data):
        super(EchoToStdout, self).write(data)
        self.process_protocol.outReceived(data)


class DefaultOutput(basic.LineReceiver):
    """ This protocol creates a baton for each line and processes it
    with the configured processor.

    The baton is a dictionary with the key *line* holding the value of
    line received.
    """
    interface.implements(IProcessOutputProtocol)

    def __init__(self, process_protocol, type, processor=None, delimiter='\n', max_length=None):
        self.process_protocol = process_protocol
        self.type = type
        self.processor_config = dict(provider=processor) if isinstance(processor, basestring) else processor

        self.delimiter = delimiter
        if max_length != None:
            self.MAX_LENGTH = max_length

        self.deferred_queue = defer.DeferredQueue()

    def configure(self, runtime_environment):
        if not self.processor_config:
            return

        dependency_manager = runtime_environment.dependency_manager

        self.puller = util.PullFromQueueAndProcessWithDependency(self.deferred_queue, self.processor_config)
        self.puller.configure(runtime_environment)

        self.puller.setName('puller.%s.%s'%(self.process_protocol.process_name, self.type))

        dependency_manager.add_dependency(self, self.puller)

        self.puller.setServiceParent(runtime_environment.application)

    def start_pulling(self):
        pass

    def lineReceived(self, line):
        baton = dict(line=line)
        if not self.processor_config:
            log.debug('Output from process %s on %s ignored because no processor were configured. Output was: %s.'%(self.process_protocol.process_name, self.type, line))
        else:
            self.deferred_queue.put(baton)

    def lineLengthExceeded(self, line):
        e_msg = 'Line length exceeded by %s. Buffer has been dropped.'%self.process_protocol.process_name
        detail = 'While reading from %s, too much data were read without encountering a delimiter.'%self.type
        hint = ('Make sure the expected delimiter are being used (currently %(delimiter)s), or adjust '
                    'the max_length value (currently %(max_length)s)')

        hint = hint%dict(delimiter=repr(self.delimiter), max_length=self.MAX_LENGTH)

        try:
            raise exceptions.PipedError(e_msg, detail, hint)
        except:
            log.error()

    def connectionMade(self):
        pass

    def processEnded(self, reason):
        pass


class RedirectToStdout(object):
    """ Relays all calls to dataReceived to process_protocol.outReceived """
    interface.implements(IProcessOutputProtocol)
    def __init__(self, process_protocol, type):
        self.process_protocol = process_protocol
        self._fail_if_this_is_stdout(type)
        self.on_baton_received = event.Event()

    def dataReceived(self, data):
        self.process_protocol.outReceived(data)

    def connectionMade(self):
        pass

    def processEnded(self, reason):
        pass

    def _fail_if_this_is_stdout(self, type):
        if type != 'stdout':
            return
        e_msg = 'stdout from process %s cannot be redirected back to stdout'%self.process_protocol.process_name
        detail = 'Attempted to redirect stdout to stdout, which would result in a loop.'
        raise exceptions.PipedError(e_msg, detail)
