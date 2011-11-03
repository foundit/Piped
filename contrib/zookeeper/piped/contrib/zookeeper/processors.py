# Copyright (c) 2010-2011, Found IT A/S and Found Project Contributors.
# All rights reserved.
import zookeeper
from zope import interface
from twisted.internet import defer

from piped import processing
from piped.processors import base


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
        """
        :param client: The name of the zookeeper client to use.
        """
        super(ZooKeeperProcessor, self).__init__(**kw)
        self.client_name = client

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.client_dependency = dm.add_dependency(self, dict(provider='zookeeper.client.%s' % self.client_name))


class GetZooKeeperChildren(ZooKeeperProcessor):
    """ Get a list of children for a ZooKeeper node. """
    interface.classProvides(processing.IProcessor)
    name = 'get-zookeeper-children'

    def __init__(self, path, cached=True, output_path='children', *a, **kw):
        """
        :param path: Path to the node.
        :param cached: Whether to cache the result.
        :param output_path: Path to store the list of children to in the baton.
        """
        super(GetZooKeeperChildren, self).__init__(*a, **kw)

        self.path = path
        self.cached = cached
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        client = yield self.client_dependency.wait_for_resource()

        # A path cannot end with a '/', unless it is the root element
        if path.endswith('/') and len(path) > 1:
            path = path[:-1]

        if self.cached:
            children = yield client.cached_get_children(path)
        else:
            children = yield client.get_children(path)

        baton = self.get_resulting_baton(baton, self.output_path, children)
        defer.returnValue(baton)


class GetZooKeeperData(ZooKeeperProcessor):
    """ Get the data of a ZooKeeper node. """
    interface.classProvides(processing.IProcessor)
    name = 'get-zookeeper-data'

    def __init__(self, path, cached=True, output_path='data', metadata_output_path=None, *a, **kw):
        """
        :param path: Path to the node.
        :param cached: Whether to cache the result.
        :param output_path: Path to store the node contents to in the baton.
        :param metadata_output_path: Path to store the node metadata to in the baton.
        """
        super(GetZooKeeperData, self).__init__(*a, **kw)

        self.path = path
        self.cached = cached
        self.metadata_output_path = metadata_output_path
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        client = yield self.client_dependency.wait_for_resource()

        if self.cached:
            data, metadata = yield client.cached_get(path)
        else:
            data, metadata = yield client.get(path)

        baton = self.get_resulting_baton(baton, self.metadata_output_path, metadata)
        baton = self.get_resulting_baton(baton, self.output_path, data)
        defer.returnValue(baton)


class ZooKeeperNodeExists(ZooKeeperProcessor):
    """ Check whether a given ZooKeeper node exists. """
    interface.classProvides(processing.IProcessor)
    name = 'zookeeper-node-exists'

    def __init__(self, path, cached=True, output_path='exists', metadata_output_path=None, *a, **kw):
        """
        :param path: Path to the node.
        :param cached: Whether to cache the result.
        :param output_path: Path to store whether the node existed or not in the baton.
        :param metadata_output_path: If the node exists, a path to store the metadata to.
        """
        super(ZooKeeperNodeExists, self).__init__(*a, **kw)

        self.path = path
        self.cached = cached
        self.metadata_output_path = metadata_output_path
        self.output_path = output_path

    @defer.inlineCallbacks
    def process(self, baton):
        path = self.get_input(baton, self.path)
        client = yield self.client_dependency.wait_for_resource()

        if self.cached:
            exists = yield client.cached_exists(path)
        else:
            exists = yield client.exists(path)

        baton = self.get_resulting_baton(baton, self.metadata_output_path, exists)
        baton = self.get_resulting_baton(baton, self.output_path, bool(exists))
        defer.returnValue(baton)


class SetZooKeeperData(ZooKeeperProcessor):
    """ Set the contents of a ZooKeeper node. """
    interface.classProvides(processing.IProcessor)
    name = 'set-zookeeper-data'

    def __init__(self, path, data, create_intermediary_nodes=False, output_path='set', **kw):
        """
        :param path: Path to the node.
        :param data: The data to set.
        :param create_intermediary_nodes: Whether to create intermediary nodes if they don't exist.
        :param output_path: Path to store the output metadata to in the baton.
        """
        super(SetZooKeeperData, self).__init__(**kw)

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
    """ Create a ZooKeeper node. """
    interface.classProvides(processing.IProcessor)
    name = 'create-zookeeper-node'

    def __init__(self, path, data=None, flags=None, create_intermediary_nodes=False, output_path='create', **kw):
        """
        :param path: Path to the node.
        :param data: The data to set.
        :param flags: A flag or a list of flags that will be used when creating the node.
            For example: [EPHEMERAL, SEQUENCE], which will create an ephemeral, sequential node.
        :param create_intermediary_nodes: Whether to create intermediary nodes id they don't exist.
        :param output_path: Path to store the output metadata to in the baton.
        """
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