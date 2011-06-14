# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.trial import unittest
from twisted.internet import defer

from piped import processing, dependencies
from piped.processors import spread_processors


class FakeRoot(object):

    def __init__(self):
        self.calls = defer.DeferredQueue()
        self.result = None

    def callRemote(self, name, *args, **kwargs):
        self.calls.put(dict(name=name, args=args, kwargs=kwargs))
        return self.result


class CallRemoteTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

        self.root = FakeRoot()
        self.root_dependency = dependencies.InstanceDependency(self.root)
        self.root_dependency.is_ready = True

    def create_processor(self, **kwargs):
        kwargs.setdefault('client', 'test_client')
        kwargs.setdefault('name', 'test_remote_name')
        processor = spread_processors.CallRemote(**kwargs)
        processor.root_dependency = self.root_dependency
        return processor

    @defer.inlineCallbacks
    def test_simple_call_remote(self):
        processor = self.create_processor()
        self.root.result = object()

        result = yield processor.process(dict())

        call = yield self.root.calls.get()
        self.assertEquals(call, dict(name='test_remote_name', args=(), kwargs=dict()))
        self.assertEquals(result, dict(result=self.root.result))

    @defer.inlineCallbacks
    def test_simple_call_remote_with_arguments(self):
        processor = self.create_processor(args_path='args', kwargs_path='kwargs')

        yield processor.process(dict(args=[1,2,3], kwargs=dict(foo='bar')))

        call = yield self.root.calls.get()
        self.assertEquals(call, dict(name='test_remote_name', args=(1,2,3), kwargs=dict(foo='bar')))

    @defer.inlineCallbacks
    def test_simple_call_remote_with_missing_arguments(self):
        processor = self.create_processor(args_path='args', kwargs_path='kwargs')

        yield processor.process(dict())

        call = yield self.root.calls.get()
        self.assertEquals(call, dict(name='test_remote_name', args=(), kwargs=dict()))