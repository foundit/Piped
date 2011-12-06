# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Utility processors that are especially useful in ZMQ contexts. """
import zmq
from twisted.internet import defer
from zope import interface

from piped import util, processing
from piped.processors import base


class MessageSender(base.Processor):
    name = 'send-zmq-message'
    interface.classProvides(processing.IProcessor)

    def __init__(self, queue_name, message_path='', retries=None, retry_wait=0.1, *a, **kw):
        super(MessageSender, self).__init__(*a, **kw)
        self.queue_name = queue_name
        self.message_path = message_path

        self.retries = retries
        self.retry_wait = retry_wait

    def configure(self, runtime_environment):
        self.dependencies = runtime_environment.create_dependency_map(self,
            socket=dict(provider='zmq.socket.%s'%self.queue_name))

    @defer.inlineCallbacks
    def process(self, baton):
        message = util.dict_get_path(baton, self.message_path)

        tries = 0

        while True:
            try:
                self.dependencies.socket.send(str(message), flags=zmq.NOBLOCK)
                defer.returnValue(baton)
            except zmq.ZMQError, ze:
                if ze.errno != zmq.EAGAIN:
                    raise

            tries += 1

            if self.retries is not None and tries >= self.retries:
                break

            if self.retries is None:
                continue

            yield util.wait(self.retry_wait)
                
        raise ze
