# Copyright (c) 2010-2011, Found IT A/S and Found Project Contributors.
# All rights reserved.
#
# This module is part of Found and is released under
# the BSD License: http://www.opensource.org/licenses/bsd-license.php
import json
from twisted.internet import defer
from twisted.web import client as web_client
from zope import interface
from piped import processing, util, yamlutil
from piped.processors import base
import zookeeper


@defer.inlineCallbacks
def _ensure_intermediary_nodes_exist(client, path):
    exists = yield client.exists(path)
    if not exists:
        # remove the last element from the path to get the parent, but ensure that the path
        # starts with a '/' even if we just removed the last element.
        parent_path = '/'+path.rsplit('/', 1)[0][1:]
        yield _ensure_intermediary_nodes_exist(client, parent_path)
        yield client.create(path, '')


class ZooKeeperProcessor(base.Processor):
    def __init__(self, client, **kw):
        super(ZooKeeperProcessor, self).__init__(**kw)
        self.client_name = client

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.client_dependency = dm.add_dependency(self, dict(provider='zookeeper.client.%s' % self.client_name))


class CachingZooKeeperProcessor(ZooKeeperProcessor):
    def __init__(self, use_cache=False, *a, **kw):
        super(CachingZooKeeperProcessor, self).__init__(*a, **kw)

        self.use_cache = use_cache
        self._func_cache = dict()

    @defer.inlineCallbacks
    def call_cached_func(self, cache_key, func, *a):
        cached = self._func_cache.get(cache_key, Ellipsis)
        if cached is not Ellipsis:
            defer.returnValue(cached)

        d, watcher = func(*a)
        watcher.addCallback(self._watch_callback, cache_key)

        result = yield d
        self._func_cache[cache_key] = result

        defer.returnValue(result)

    def _watch_callback(self, event, cache_key):
        # invalidate our local cache on any callback
        del self._func_cache[cache_key]


class GetZooKeeperChildren(CachingZooKeeperProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'get-zookeeper-children'

    def __init__(self, path, output_path='children', *a, **kw):
        super(GetZooKeeperChildren, self).__init__(*a, **kw)

        self.path = path
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        client = yield self.client_dependency.wait_for_resource()

        if path.endswith('/'):
            path = path[:-1]

        if self.use_cache:
            cache_key = '{0}/{1}'.format(id(client), path)
            children = yield self.call_cached_func(cache_key, client.get_children_and_watch, path)
        else:
            children = yield client.get_children(path)

        baton = self.get_resulting_baton(baton, self.output_path, children)
        defer.returnValue(baton)


class GetZooKeeperData(CachingZooKeeperProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'get-zookeeper-data'

    def __init__(self, path, output_path='data', metadata_output_path=None, *a, **kw):
        super(GetZooKeeperData, self).__init__(*a, **kw)

        self.path = path
        self.metadata_output_path = metadata_output_path
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        client = yield self.client_dependency.wait_for_resource()

        if self.use_cache:
            cache_key = '{0}/{1}'.format(id(client), path)
            data, metadata = yield self.call_cached_func(cache_key, client.get_and_watch, path)
        else:
            data, metadata = yield client.get(path)

        baton = self.get_resulting_baton(baton, self.metadata_output_path, metadata)
        baton = self.get_resulting_baton(baton, self.output_path, data)
        defer.returnValue(baton)


class ZooKeeperNodeExists(CachingZooKeeperProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'zookeeper-node-exists'

    def __init__(self, path, output_path='exists', metadata_output_path=None, *a, **kw):
        super(ZooKeeperNodeExists, self).__init__(*a, **kw)

        self.path = path

        self.metadata_output_path = metadata_output_path
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        client = yield self.client_dependency.wait_for_resource()

        if self.use_cache:
            cache_key = '{0}/{1}'.format(id(client), path)
            exists = yield self.call_cached_func(cache_key, client.exists_and_watch, path)
        else:
            exists = yield client.exists(path)

        baton = self.get_resulting_baton(baton, self.metadata_output_path, exists)
        baton = self.get_resulting_baton(baton, self.output_path, bool(exists))
        defer.returnValue(baton)


class SetZooKeeperNode(ZooKeeperProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'set-zookeeper-node'

    def __init__(self, path, data, create_intermediary_nodes=False, output_path='set', **kw):
        super(SetZooKeeperNode, self).__init__(**kw)

        self.path = path
        self.data = data

        self.create_intermediary_nodes = create_intermediary_nodes

        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        data = self.get_input(baton, self.data)

        client = yield self.client_dependency.wait_for_resource()

        if self.create_intermediary_nodes:
            yield _ensure_intermediary_nodes_exist(client, path)
        
        result = yield client.set(path, data)

        baton = self.get_resulting_baton(baton, self.output_path, result)
        defer.returnValue(baton)


class CreateZooKeeperNode(ZooKeeperProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'create-zookeeper-node'

    def __init__(self, path, data=None, flags=None, create_intermediary_nodes=False, output_path='node', **kw):
        super(CreateZooKeeperNode, self).__init__(**kw)

        self.path = path
        self.data = data
        self.flags = self._parse_flags(flags or list())
        self.create_intermediary_nodes = create_intermediary_nodes

        self.output_path = output_path

    def _parse_flags(self, flags):
        if isinstance(flags, basestring):
            flags = [flags]

        computed = 0

        for flag in flags:
            if isinstance(flag, basestring):
                flag = getattr(zookeeper, flag)
            computed |= flag

        return computed

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        data = self.get_input(baton, self.data)
        client = yield self.client_dependency.wait_for_resource()

        if self.create_intermediary_nodes:
            yield _ensure_intermediary_nodes_exist(client, '/'+path.rsplit('/', 1)[0][1:])

        created = yield client.create(self.path, data, flags=self.flags)

        baton = self.get_resulting_baton(baton, self.output_path, created)
        defer.returnValue(baton)