# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import sys

from twisted.application import service
from twisted.internet import defer, stdio, process, reactor, fdesc
from twisted.protocols import basic
from zope import interface

from piped import event, exceptions, log, util, resource


class StdinProvider(object, service.MultiService):
    """ Provides a batons from stdin.

    Configuration example::

        stdin: # defaults
            delimiter: "\n"
            max_length: 16384
            processor: processor_name

    The batons are on the format ``dict(line=received_line)``.
    
    A baton with line set to ``None`` means the connection to stdin was closed and
    no further batons will be produced by this provider.
    """
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        service.MultiService.__init__(self)
        self.on_start = event.Event()
        self.on_pause = event.Event()

        self.deferred_queue = defer.DeferredQueue()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.setServiceParent(runtime_environment.application)
        self.dependency_manager = runtime_environment.dependency_manager

        stdin_config = runtime_environment.get_configuration_value('stdin', dict(enabled=False))

        if not stdin_config.pop('enabled', True):
            return

        processor = stdin_config.pop('processor')
        self.processor = dict(provider=processor) if isinstance(processor, basestring) else processor

        self.protocol = StdinProtocol(self.deferred_queue, **stdin_config)
        self.factory = StandardIO(self.protocol)

        self.puller = util.PullFromQueueAndProcessWithDependency(self.deferred_queue, self.processor)
        self.puller.setName('stdin.lines')
        self.puller.configure(runtime_environment)
        self.puller.setServiceParent(self)


# We use this process.ProcessReader subclass in order to avoid messing up
# the standard input/output for other users, such as twisted.log
class StandardIOProcessReader(process.ProcessReader):
    """ A process reader that sets its file descriptor as blocking. """
    def __init__(self, *a, **kw):
        process.ProcessReader.__init__(self, *a, **kw)
        fdesc.setBlocking(sys.stdin.fileno())


class StandardIO(stdio.StandardIO):
    def __init__(self, proto):
        self.protocol = proto
        self._reader= StandardIOProcessReader(reactor, self, 'read', sys.stdin.fileno())
        self.protocol.makeConnection(self)


class StdinException(exceptions.PipedError):
    pass


class StdinProtocol(basic.LineReceiver):
    """ This protocol creates a baton for each line and puts it into the deferred_queue. """

    def __init__(self, deferred_queue, delimiter='\n', max_length=16384):
        self.deferred_queue = deferred_queue

        self.delimiter = delimiter
        self.MAX_LENGTH = max_length

    def connectionLost(self, reason):
        baton = dict(line=None)
        self.deferred_queue.put(baton)

    def lineReceived(self, line):
        baton = dict(line=line)
        self.deferred_queue.put(baton)

    def lineLengthExceeded(self, line):
        e_msg = 'Line length exceeded. Buffer has been dropped.'
        hint = 'While reading from stdin, too much data were read without encountering a delimiter.'
        detail = ('Make sure the expected stdin.delimiter are being used (currently %s), or adjust '
                    'the stdin.max_length value (currently %s)')

        detail = detail%(repr(self.delimiter), self.MAX_LENGTH)

        try:
            raise StdinException(e_msg, hint, detail)
        except:
            log.error()
