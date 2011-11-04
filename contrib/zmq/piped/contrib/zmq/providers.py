# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import threading
import time

import zmq
from twisted.application import service
from twisted.internet import reactor, defer, task
from zope import interface

from piped import exceptions, log, resource, dependencies, util


class SocketError(exceptions.PipedError):
    """ Raised when there's a problem with a ZeroMQ-socket. """


class NoSuchSocketError(SocketError):
    pass


class ZMQSocketProvider(object):
    """ I provide raw ZMQ-sockets with the paths "zmq.socket.queue_name".

    I expect sockets to be configured like the following: ::

        zmq:
            queues:
                queue_name:
                    type: PULL/PUSH/etc.
                    sockopts:
                        - key: HWM
                          value: 100
                    connects:
                        - protocol://interface:port
                        - protocol://interface:port
                    binds:
                        - protocol://interface:port
                        - protocol://interface:port

    The type must be a valid ZeroMQ socket type.

    """
    interface.classProvides(resource.IResourceProvider)

    context_factory = zmq.Context
    _context = None

    def __init__(self):
        ## TODO: When the state changes to stopping, etc, set
        # high-water marks and stuff.
        self._socket_by_name = dict()
        self._name_by_socket = dict()
        self._sockets_to_bind = []
        self._sockets_to_connect = []
        self._sockets_with_sockopts = []
        self._events_and_their_sockets = []

    @property
    def queues(self):
        return self.runtime_environment.get_configuration_value('zmq.queues', dict())

    @property
    def context(self):
        if not self._context:
            self._context = self.context_factory()
        return self._context

    def __del__(self):
        # It's really important to close sockets before the context is
        # attempted terminated (which it is when it's garbage
        # collected). Any open sockets will block the termination,
        # effectively causing the entire Python-process to hang.
        for socket_name, socket in self._socket_by_name.items():
            try:
                socket.close()
            except:
                # Nothing we can do, but we'll want to try to close the other sockets.
                log.error('Could not close ZeroMQ-socket named "%s"' % socket_name)
                util.reraise_if_should_exit()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager

        resource_manager = runtime_environment.resource_manager

        # We can provide the `zmq.Context`. The context is created lazily by the
        # :func:`context` property in order to not avoid starting unnecessary threads.
        resource_manager.register('zmq.context', provider=self)

        # State that we can provide all the sockets.
        for socket_name, socket_configuration in self.queues.items():
            self._fail_if_socket_configuration_is_invalid(socket_configuration)
            resource_manager.register('zmq.socket.%s'%socket_name, provider=self)

        self_dependency = self.dependency_manager.as_dependency(self)
        # TODO: Fix start_providing, so it'll be possible to add more sockets post initial configure.
        self_dependency.on_ready.wait_until_fired().addCallback(lambda _: reactor.callLater(0, self.start_providing))

    def add_consumer(self, resource_dependency):
        type = resource_dependency.provider.split('.')[1]

        if type == 'context':
            resource_dependency.on_resource_ready(self.context)
            return

        socket_name = resource_dependency.provider.split('.')[2]

        self._fail_if_socket_name_is_invalid(socket_name)

        if socket_name in self._socket_by_name:
            socket = self._socket_by_name[socket_name]
        else:
            socket = self._socket_by_name[socket_name] = self._create_socket(socket_name)
            self._name_by_socket[socket] = socket_name

            # By making the socket depend on the provider, it won't
            # start until the provider says it's ready. The provider
            # itself has no dependencies, so it's ready upon
            # resolve. Our on_ready triggers start_providing. See
            # configure() for how that's wired.
            self.dependency_manager.add_dependency(socket, self)

        self.dependency_manager.add_dependency(resource_dependency, socket)

        # Invoke the events with their respective sockets on start_providing()
        self._events_and_their_sockets.append((resource_dependency.on_resource_ready, socket))

    @classmethod
    def _fail_if_socket_configuration_is_invalid(cls, socket_configuration):
        cls._fail_if_socket_type_is_invalid(socket_configuration['type'])
        # TODO: We can do more here, such as reasoning on the
        # compatability of the socket compositions --- e.g. fail if
        # inproc-sockets are missing or warn on weird socket-type-combinations.

    @classmethod
    def _fail_if_socket_type_is_invalid(cls, socket_type):
        if socket_type not in ('REQ', 'REP', 'PUB', 'SUB', 'PAIR', 'XREQ', 'XREP', 'PULL', 'PUSH'):
            raise SocketError('invalid socket type "%s"' % socket_type)

    def _fail_if_socket_name_is_invalid(self, socket_name):
        socket_names = self.queues.keys()
        if socket_name in socket_names:
            return

        e_msg = 'no such socket: ' + socket_name
        detail = 'Available sockets: "%s" ' % '", "'.join(socket_names)
        raise NoSuchSocketError(e_msg, detail)

    def _create_socket(self, socket_name):
        socket_config = self.queues[socket_name]
        socket_type = getattr(zmq, socket_config['type'])
        socket = self.context.socket(socket_type)

        # Connect and bind.  We care about the order in which sockets
        # are bound/connected, so we defer binding/connecting until
        # start_providing.
        self._sockets_to_bind.append((socket, socket_config.get('binds', [])))
        self._sockets_to_connect.append((socket, socket_config.get('connects', [])))
        self._sockets_with_sockopts.append((socket, socket_config.get('sockopts', dict())))

        return socket

    def start_providing(self):
        # It's important that binds are bound before the connects, in
        # case of INPROC-sockets.
        self._configure_sockopts()
        self._configure_binds()
        self._configure_connects()
        self._provide_consumers_with_their_sockets()

    def _configure_binds(self):
        for socket, bind_specs in self._sockets_to_bind:
            for bind_spec in bind_specs:
                log.info('Binding %s to %s.'%(self._name_by_socket[socket], bind_spec))
                socket.bind(bind_spec)
        self._sockets_to_bind = []

    def _configure_connects(self):
        for socket, connect_specs in self._sockets_to_connect:
            for connect_spec in connect_specs:
                log.info('Connecting %s to %s.'%(self._name_by_socket[socket], connect_spec))
                socket.connect(connect_spec)
        self._sockets_to_connect = []

    def _configure_sockopts(self):
        for socket, sockopts in self._sockets_with_sockopts:
            for sockopt in sockopts:
                sockopt_key = sockopt['key']
                sockopt_value = sockopt['value']

                socket.setsockopt(getattr(zmq, sockopt_key, sockopt_key), sockopt_value)

    def _provide_consumers_with_their_sockets(self):
        # These are events for a specific (dependency -> dependent)-edge in the dependency-graph.
        for event, socket in self._events_and_their_sockets:
            event(socket)


class ZMQProcessorFeederProvider(object):
    """ I create ZMQProcessorFeeders for zmq queues with processors.

    Example configuration::

        zmq:
            poll_timeout: 100 # poll timeout in milliseconds
            queues:
                queue_name:
                    processor: processor_name
    """
    interface.classProvides(resource.IResourceProvider)

    _socket_poller = None

    @property
    def queues(self):
        return self.runtime_environment.get_configuration_value('zmq.queues', dict())

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.dependency_manager = runtime_environment.dependency_manager

        # TODO: proper stopping
        #self._bind_events_on_state_machine_changes()

        for queue_name, queue_configuration in self.queues.items():
            if 'processor' not in queue_configuration:
                # We only care about queues that should feed a processor.
                continue

            processor_config = queue_configuration['processor']

            feeder = ZMQProcessorFeeder(self._get_or_create_socket_poller(), processor_config, queue_name)
            feeder.configure(self.runtime_environment)

    def _get_or_create_socket_poller(self):
        """ A wrapper to lazily create the socket poller, in order to
        avoid starting it without any queues to poll. """
        if not self._socket_poller:
            self._socket_poller = ZMQSocketPoller()
            self._socket_poller.configure(self.runtime_environment)
        return self._socket_poller


class ZMQProcessorFeeder(object):
    """
    This class takes care of registering itself in a ZMQSocketPoller
    in order to process incoming messages as batons in a processor.
    """
    def __init__(self, socket_poller, processor_config, socket_name):
        self.socket_poller = socket_poller
        self.processor_config = dict(provider=processor_config) if isinstance(processor_config, basestring) else processor_config
        self.socket_name = socket_name

    def configure(self, runtime_environment):
        self.dependency_manager = runtime_environment.dependency_manager

        self.processor_dependency = self.dependency_manager.add_dependency(self, self.processor_config)

        # I need the zmq socket to operate
        self.socket_dependency = dependencies.ResourceDependency(provider='zmq.socket.%s'%self.socket_name)
        self.dependency_manager.add_dependency(self, self.socket_dependency)

        # I also require the poller to be functional
        self.dependency_manager.add_dependency(self, self.socket_poller)

        # and when I'm ready, I should start handling messages:
        self_dependency = self.dependency_manager.as_dependency(self)
        self_dependency.on_ready.wait_until_fired().addCallback(lambda _: self._register_in_poller())

    def _log_process_error(self, reason):
        log.error(reason)

    def _register_in_poller(self):
        self.socket_poller.register_socket(self, self.socket_dependency.get_resource())

    @defer.inlineCallbacks
    def handle_messages(self, message_batch):
        try:
            for message in message_batch:
                try:
                    processor = yield self.processor_dependency.wait_for_resource()
                    # TODO: We ought to have a cooperator somewhere
                    # else, for all the processors regardless of where
                    # the batons come from.
                    cooperative = task.cooperate(iter([defer.maybeDeferred(processor, message).addErrback(self._log_process_error)]))
                    yield cooperative.whenDone()
                except Exception:
                    log.error()
        finally:
            self._register_in_poller()


class ZMQSocketPoller(service.Service):
    """  I am a thread that deals with

        * polling multiple sockets to get hold of the messages

        * de-registering sockets from the poller that currently has
          messages that are under processing. This is important to
          ensure that the queues fill to the high watermark, so ZMQ
          stops accepting more messages.

        * providing batches of messages to the `ZMQProcessorFeeder`s.

    These indirections are to ensure that the reactor is not swamped
    when lots of messages are received --- and to avoid a thread per socket.
    """
    poller_factory = zmq.Poller
    maximum_batch_size = 10
    poll_timeout = 100 # milliseconds
    thread = None

    def __init__(self):
        self._feeder_by_socket = dict()
        self._poller = self.poller_factory()
        self._sockets_to_register = list()

        self._control_socket_push = None
        self._control_socket_pull = None

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        self.dependency_manager = runtime_environment.dependency_manager

        self.maximum_batch_size = runtime_environment.get_configuration_value('zmq.maximum_batch_size', self.maximum_batch_size)
        self.poll_timeout = runtime_environment.get_configuration_value('zmq.poll_timeout', self.poll_timeout)

        context_resource = dependencies.ResourceDependency(provider='zmq.context')
        context_resource.on_ready += lambda _: self._create_control_socket(context_resource.get_resource())

        self.dependency_manager.add_dependency(self, context_resource)

        runtime_environment.resource_manager.register('zmq.poller', provider=self)

    def add_consumer(self, resource_dependency):
        resource_dependency.on_resource_ready(self)

    def startService(self):
        service.Service.startService(self)
        if not self.thread:
            self.thread = threading.Thread(target=self.run)
            self.thread.start()

    def stopService(self):
        service.Service.stopService(self)
        if self._control_socket_push:
            # send a shutdown message through the control socket, in order to wake up the poller
            self._control_socket_push.send('shutdown')

        if self.thread:
            self.thread.join()
            self.thread = None

    def _create_control_socket(self, context):
        # We use a "control socket" to control the poller --- so we can wake it up when we need to.
        self._control_socket_push = context.socket(zmq.PUSH)
        self._control_socket_pull = context.socket(zmq.PULL)

        # Make sure the socket-names are unique, in the unlikely event of multiple pollers.
        self._control_socket_push.bind('inproc://socket_poller_%s'%id(self))
        self._control_socket_pull.connect('inproc://socket_poller_%s'%id(self))

        self._sockets_to_register.append(self._control_socket_pull)

    def register_socket(self, feeder, socket):
        self._feeder_by_socket[socket] = feeder
        self._sockets_to_register.append(socket)

        # If the control socket has been created, send it a message,
        # to wake up the poller. This is important in case the poller
        # is stuck in a poll()-call. We need to make it do another
        # iteration to register new sockets.
        if self._control_socket_push:
            self._control_socket_push.send('socket registered')

    def _register_pending_sockets(self):
        while self._sockets_to_register:
            socket = self._sockets_to_register.pop()
            self._poller.register(socket, zmq.POLLIN)

    def run(self):
        while self.running:
            # if we have any sockets that needs registering in the poller, register them before we begin the poll
            self._register_pending_sockets()

            # Don't spin out of control if there are no sockets to poll
            if not self._poller.sockets:
                time.sleep(self.poll_timeout/1000.)
                continue

            for polled_socket, polled_type in self._poller.poll():
                # Get hold of a batch of messages. We deal with
                # batches to amortize the overhead of getting them.
                message_batch = list()
                try:
                    for i in range(self.maximum_batch_size):
                        message_batch.append(polled_socket.recv(zmq.NOBLOCK))
                except zmq.ZMQError, e:
                    if e.errno != zmq.EAGAIN:
                        raise
                    # We've emptied the queue. That's fine!

                if polled_socket == self._control_socket_pull:
                    # it was just our control socket. do nothing
                    continue

                # Don't poll more messages from this socket until it
                # is re-registered.  With no messages getting polled
                # out of it, it'll eventually hit the high watermark,
                # making sure that upstream sockets don't deliver us
                # more messages until we're ready to accept them.
                self._poller.unregister(polled_socket)

                # Deliver the messages to whatever wants to process them.
                feeder = self._feeder_by_socket[polled_socket]
                reactor.callFromThread(feeder.handle_messages, message_batch)
