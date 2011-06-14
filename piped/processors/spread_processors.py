# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from zope import interface
from twisted.internet import defer

from piped import util, processing
from piped.processors import base


class CallRemote(base.Processor):
    """ Calls a remote function using PB. """
    interface.classProvides(processing.IProcessor)
    name = 'pb-call-remote'

    def __init__(self, client, name, args_path=None, kwargs_path=None, output_path='result', **kw):
        super(CallRemote, self).__init__(**kw)
        self.client_name = client
        
        self.remote_name = name
        self.args_path = args_path
        self.kwargs_path = kwargs_path

        self.output_path = output_path

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.root_dependency = dm.add_dependency(self, dict(provider='pb.client.%s.root_object' % self.client_name))

    @defer.inlineCallbacks
    def process(self, baton):
        root = yield self.root_dependency.wait_for_resource()

        args = util.dict_get_path(baton, self.args_path, list()) if self.args_path is not None else list()
        kwargs = util.dict_get_path(baton, self.kwargs_path, dict()) if self.kwargs_path is not None else dict()

        result = yield root.callRemote(self.remote_name, *args, **kwargs)

        if self.output_path == '':
            defer.returnValue(result)

        util.dict_set_path(baton, self.output_path, result)
        defer.returnValue(baton)
