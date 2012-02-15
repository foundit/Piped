# Copyright (c) 2010-2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
"""
This module contains base implementations and managers for the resource system.
"""
import copy
import networkx
from twisted.internet import defer
from twisted.python import failure, components
from zope import interface

from piped import event, exceptions


class IDependency(interface.Interface):
    """ Interface that represents something that can act as dependency.

    Dependencies are either created implicitly by a DependencyManager,
    or explicitly by a user.
    """

    on_ready = interface.Attribute("""
        :class:`piped.event.Event` that should be invoked with ``self``
        when the dependency is considered ready.
    """)

    on_lost = interface.Attribute("""
        :class:`piped.event.Event` that should be invoked with `self, reason`
        when the dependency is lost.
    """)

    on_dependency_ready = interface.Attribute("""
        :class:`piped.event.Event` invoked by the :class:`DependencyManager`
        with ``dependency`` when one of the dependencies of this dependency becomes ready.

        This gives the instance a chance to consider whether it is
        ready. If so, ``on_ready`` must be invoked.
    """)

    on_dependency_lost = interface.Attribute("""
        :class:`piped.event.Event` invoked by the :class:`DependencyManager`
        with ``dependency, reason`` when one of the dependencies of this dependency are lost.
    """)

    manager = interface.Attribute("""
        This attribute is used by :class:`DependencyManager`\s to
        ensure that a dependency is only interacting with a single manager.
    """)

    def configure(self, runtime_environment):
        """ Configure the dependency.

        :param runtime_environment: The current :class:`.processing.RuntimeEnvironment`.
        """

    def resolve_initial_state(self):
        """ Determine the initial state of this dependency.

        This function is called by the :class:`DependencyManager` *only once* when
        the current dependencies have been added to this `IDependency`.
        """

    def get_resource(self):
        """ Return the resource that is represented by this dependency,
        or raise an `UnprovidedResourceError`. """


class Dependency(object):
    """ Base class for dependencies. """
    interface.implements(IDependency)
    is_ready = False

    def __init__(self, manager=None, cascade_ready=True, cascade_lost=True):
        """
        For the description of the instance events, see `piped.resource.IDependency`

        :ivar on_ready: :class:`piped.event.Event`
        :ivar on_lost: :class:`piped.event.Event`
        :ivar on_dependency_ready: :class:`piped.event.Event` invoked by the `DependencyManager`
        :ivar on_dependency_lost: :class:`piped.event.Event` invoked by the `DependencyManager`
        """
        self.manager = manager

        self.on_ready = event.Event()
        self.on_ready += lambda d: setattr(self, 'is_ready', True)
        self.on_lost = event.Event()
        self.on_lost += lambda d, r: setattr(self, 'is_ready', False)

        self.on_dependency_ready = event.Event()
        self.on_dependency_lost = event.Event()

        self.on_dependency_ready += lambda dependency: self._consider_cascading_ready()
        self.on_dependency_lost += lambda dependency, reason: self._consider_cascading_lost(reason)

        self.cascade_ready = cascade_ready
        self.cascade_lost = cascade_lost

    def configure(self, runtime_environment):
        pass

    def _is_resolved(self):
        if self.manager:
            return self.manager.has_resolved(self)
        return False

    def _consider_cascading_ready(self):
        if not self._is_resolved():
            return

        if self.cascade_ready and not self.is_ready:
            self._fire_on_ready_if_all_dependencies_are_provided()

    def _consider_cascading_lost(self, reason):
        if not self._is_resolved():
            return

        if self.cascade_lost and self.is_ready:
            self.fire_on_lost(reason)

    def _fire_on_ready_if_all_dependencies_are_provided(self):
        if not self.manager.has_all_dependencies_provided(self):
            return
        self.fire_on_ready()

    def fire_on_ready(self):
        self.on_ready(self)

    def fire_on_lost(self, reason):
        self.on_lost(self, reason)

    def resolve_initial_state(self):
        """ Resolves the initial state of this dependency.

        If the dependency has all its dependencies satisfied and `cascade_ready` is true,
        `on_ready` is fired.

        Otherwise, if `cascade_lost` is true, `on_lost` is fired.
        """
        if self.manager.has_all_dependencies_provided(self) and self.cascade_ready:
            self.fire_on_ready()
        elif self.cascade_lost:
            self.fire_on_lost(failure.Failure(('%r' % self,), exceptions.InitiallyLost))


class InstanceDependency(Dependency):
    """ Represents a dependency on a specific instance. """

    def __init__(self, instance, *args, **kwargs):
        super(InstanceDependency, self).__init__(*args, **kwargs)
        self.instance = instance

    def get_resource(self):
        """ Get the resource this dependency represents. """
        return self.instance

    @defer.inlineCallbacks
    def wait_for_resource(self, timeout=None):
        """ Returns a deferred that fires with the resource when it becomes available.

        If this dependency is ready and the resource is available, the returned
        deferred will already have been callbacked with the resource.

        :param timeout: Time in seconds before a :class:`~piped.exceptions.TimeoutError`
            should be raised if the resource is not ready. Defaults to ``None``, which
            means no timeout.
        """
        if self.is_ready:
            defer.returnValue(self.get_resource())

        # wait for the next on ready call
        yield self.on_ready.wait_until_fired(timeout=timeout)
        defer.returnValue(self.get_resource())

    def __repr__(self):
        if getattr(self.instance, 'name', None):
            return 'Instance(%s at %s)' % (self.instance.name, hex(id(self.instance)))
        return 'Instance(%r)' % self.instance


NO_RESOURCE = object()


class ResourceDependency(Dependency):
    """ Represents a dependency on a resource. """
    _resource = NO_RESOURCE

    def __init__(self, provider, configuration=None, *args, **kwargs):
        """ Initializes this dependency with a provider and an optional configuration.

        :param provider: A *resource_path*

        :param configuration: Additional configuration options that the
            :class:`piped.resource.IResourceProvider` may use when resolving this dependency.


        .. method:: on_resource_ready(resource)

            an :class:`piped.event.Event` that should be invoked by the provider with the
            resource when the resource becomes ready.

        .. method:: on_resource_lost(reason)

            an :class:`piped.event.Event` that should be invoked by the provider
            with a :class:`twisted.python.failure.Failure` instance
            that may explain why the resource was lost.
        """
        super(ResourceDependency, self).__init__(*args, **kwargs)
        self.provider = provider
        self.configuration = configuration or dict()

        # when the resource becomes ready or lost, update self.resource and consider cascading
        self.on_resource_ready = event.Event()
        self.on_resource_ready += lambda resource: setattr(self, '_resource', resource)
        self.on_resource_ready += lambda resource: self._consider_cascading_ready()

        self.on_resource_lost = event.Event()
        self.on_resource_lost += lambda reason: setattr(self, '_resource', NO_RESOURCE)
        self.on_resource_lost += lambda reason: self._consider_cascading_lost(reason)

        # TODO: add tests for resource_lost testing

    def configure(self, runtime_environment):
        self.resource_manager = runtime_environment.resource_manager

    def get_resource(self):
        """ Get the resource this dependency represents.

        :returns: the represented resource.
        :raises: `exceptions.UnprovidedResourceError` if the resource isn't ready.

        Note that this will return the resource even if the dependency itself isn't ready,
        as it may still depend on other dependencies.
        """
        if self._resource == NO_RESOURCE:
            raise exceptions.UnprovidedResourceError('%r has no resource.' % self)
        return self._resource

    @defer.inlineCallbacks
    def wait_for_resource(self, timeout=None):
        """ Returns a deferred that fires with the resource when it becomes available.

        If this dependency is ready and the resource is available, the returned
        deferred will already have been callbacked with the resource.

        :param timeout: Time in seconds before a :class:`~piped.exceptions.TimeoutError`
            should be raised if the resource is not ready. Defaults to ``None``, which
            means no timeout.
        """
        if self.is_ready:
            defer.returnValue(self.get_resource())

        # wait for the next on ready call
        yield self.on_ready.wait_until_fired(timeout=timeout)
        defer.returnValue(self.get_resource())

    def _fire_on_ready_if_all_dependencies_are_provided(self):
        if self._resource == NO_RESOURCE:
            return # we're not ready if we're missing the resource
        super(ResourceDependency, self)._fire_on_ready_if_all_dependencies_are_provided()

    def resolve_initial_state(self):
        """ Resolves the initial state of this resource dependency.

        Uses the `ResourceManager` from the runtime environment to resolve itself. If
        the resolve does not results in an immediate call to :func:`on_resource_ready`, this
        function will fire :func:`on_lost`.
        """
        self.resource_manager.resolve(self)

        if self._resource == NO_RESOURCE:
            # if add_consumer didn't result in an immediate resource, we consider ourselves down
            self.fire_on_lost(failure.Failure(('Initial state is unprovided',), exceptions.InitiallyLost))

    def __repr__(self):
        keys_values = ','.join('%s=%r' % (key, value) for key, value in self.configuration.items())
        configuration_string = ''
        if keys_values:
            configuration_string = ',' + keys_values
        return 'Resource(%r%s at %s)' % (self.provider, configuration_string, hex(id(self)))


class DictResourceDependencyAdapter(ResourceDependency):
    """ Adapter that enables just providing a dictionary to configure
    the resource dependency.
    """

    def __init__(self, resource_configuration, *a, **kw):
        if not 'provider' in resource_configuration:
            e_msg = 'Invalid resource configuration.'
            detail = 'A resource configuration must at least contain the key "provider". The adapter was provided: %r.' % resource_configuration
            raise exceptions.ResourceError(e_msg, detail)

        # copy the resource configuration since we're going to edit it:
        resource_configuration = copy.copy(resource_configuration)
        provider = resource_configuration.pop('provider')

        super(DictResourceDependencyAdapter, self).__init__(provider, resource_configuration, *a, **kw)


components.registerAdapter(DictResourceDependencyAdapter, dict, IDependency)


class DependencyManager(object):
    """ Manages a directed, acyclic graph of dependencies. """

    def __init__(self):
        self._dependency_graph = networkx.DiGraph()
        self._instance_dependencies = dict()

    def configure(self, runtime_environment):
        """ Configures this manager with the runtime environment.

        :type runtime_environment: `.processing.RuntimeEnvironment`
        """
        self.runtime_environment = runtime_environment

    def _get_instance_dependency(self, instance):
        if instance not in self._instance_dependencies:
            dependency = InstanceDependency(instance, self)
            self._instance_dependencies[instance] = dependency

        return self._instance_dependencies[instance]

    def has_resolved(self, dependency):
        """ Return whether the `DependencyManager` has resolved *dependency*. """
        self._bind_dependency_to_graph(dependency)
        return self._dependency_graph.node[dependency]['resolved']

    def _bind_dependency_to_graph(self, dependency):
        self._fail_if_bound_to_other_dependency_manager(dependency)
        dependency.manager = self

        if not dependency in self._dependency_graph:
            self._dependency_graph.add_node(dependency, resolved=False, ready=None)

            # configure the dependency with our runtime environment,
            # letting the dependency get the resource manager, the
            # dependency manager, etc. This enables it to request its
            # own dependencies and/or resources provided.
            dependency.configure(self.runtime_environment)

            # we store the last on_ready the dependency has fired with in order to replay
            # it to consumers that are added to it after the dependency has been initially
            # resolved.
            node_data = self._dependency_graph.node[dependency]
            dependency.on_ready += lambda dependency: node_data.__setitem__('ready', dependency)
            dependency.on_lost += lambda dependency, reason: node_data.__setitem__('ready', None)

    def _fail_if_bound_to_other_dependency_manager(self, dependency):
        assert dependency.manager in [self, None] # TODO: Raise a better exception.

    def as_dependency(self, maybe_dependency):
        """ Adapts an object into the `piped.resource.IDependency` interface.

        :raises: An exception may be raised if the object has a registered
            adapter, but the adapter raised an exception in its __init__.
        :rtype: An object implementing `piped.resource.IDependency`
        """
        try:
            dependency = IDependency(maybe_dependency)
        except TypeError, te:
            if 'Could not adapt' not in te.args:
                raise
            # if we cannot adapt the object, we consider it an instance dependency.
            dependency = self._get_instance_dependency(maybe_dependency)
        return dependency

    def add_dependency(self, consumer, dependency=None):
        """ Adds a dependency to the dependency graph.

        This function may be called with only one argument, which serves the purpose of
        binding a dependency to this manager and making it resolve during the next
        call to :func:`resolve_initial_states`.

        Any input types accepted by :func:`as_dependency` are accepted by this function.

        :param consumer: The consumer dependency.
        :param dependency: The dependency the consumer requires.
        """
        consumer = self.as_dependency(consumer)
        self._bind_dependency_to_graph(consumer)

        if not dependency:
            # just adding a single node, so we're done.
            return

        dependency = self.as_dependency(dependency)
        self._bind_dependency_to_graph(dependency)

        self._fail_if_dependency_already_exists(dependency, consumer)

        self._dependency_graph.add_edge(dependency, consumer, provided=False)

        self._wire_events_for_consumer_and_dependency(dependency, consumer)

        # on_ready may already have been called by the dependency, and it will not call on_ready/on_lost again
        # until its state changes, so we replay the previous on_ready or on_lost event to the consumer
        self._replay_ready_event_if_required(consumer, dependency)
        return dependency

    def _wire_events_for_consumer_and_dependency(self, dependency, consumer):
        # Save the references to the callbacks so we may remove them later. (See remove_dependency)
        edge_data = self._dependency_graph.get_edge_data(dependency, consumer)
        edge_data['on_ready_handler'] = lambda dependency: self._consider_cascading_ready(dependency, consumer)
        edge_data['on_lost_handler'] = lambda dependency, reason: self._consider_cascading_lost(dependency, consumer, reason=reason)
        dependency.on_ready += edge_data['on_ready_handler']
        dependency.on_lost += edge_data['on_lost_handler']

    def _fail_if_dependency_already_exists(self, dependency, consumer):
        if self._dependency_graph.has_edge(dependency, consumer):
            e_msg = 'Dependency relationship already exists.'
            detail = 'A dependency relationship from %r to %r already exists in this graph.' % (dependency, consumer)
            raise exceptions.AlreadyExistingDependency(e_msg, detail)

    def remove_dependency(self, consumer, dependency):
        """ Removes a dependency from the dependency graph. """
        consumer = self.as_dependency(consumer)
        dependency = self.as_dependency(dependency)

        # remove our handlers
        edge_data = self._dependency_graph.get_edge_data(dependency, consumer)
        dependency.on_ready -= edge_data['on_ready_handler']
        dependency.on_lost -= edge_data['on_lost_handler']

        # remove the edge from the graph
        self._dependency_graph.remove_edge(dependency, consumer)

        # If the dependency was lost, we pretend that it is ready so
        # that the consumer gets a chance to become ready. This might
        # seem counter-intuitive, but the point is to give the
        # consumer a chance to consider whether it is ready due to
        # having one less dependency.
        dependency_node_data = self._dependency_graph.node[dependency]
        if not dependency_node_data['ready']:
            consumer.on_dependency_ready(dependency)

    def _replay_ready_event_if_required(self, consumer, dependency):
        node_data = self._dependency_graph.node[dependency]

        # the callbacks that update this information are attached in _bind_dependency_to_graph
        ready = node_data.get('ready', None)

        if ready:
            self._consider_cascading_ready(dependency, consumer)
        else:
            reason = failure.Failure(('No exception stored',), exceptions.ReplayedLost)
            self._consider_cascading_lost(dependency, consumer, reason=reason)

    def _consider_cascading_ready(self, dependency, consumer):
        edge_data = self._dependency_graph[dependency][consumer]
        if edge_data['provided']:
            # no change in the status, so no cascading necessary
            return
        edge_data['provided'] = True

        if not self.has_resolved(dependency):
            # It's not ready to receive events from us, but it'll get
            # a chance later on when it's resolved.
            return

        consumer.on_dependency_ready(dependency)

    def _consider_cascading_lost(self, dependency, consumer, reason):
        edge_data = self._dependency_graph[dependency][consumer]
        if not edge_data['provided']:
            # no change in the status, so no cascading necessary
            return
        edge_data['provided'] = False

        if not self.has_resolved(dependency):
            # It's not ready to receive events from us, but it'll get
            # a chance later on when it's resolved.
            return

        consumer.on_dependency_lost(dependency, reason)

    def has_all_dependencies_provided(self, consumer):
        """ Return whether *consumer* has all its dependencies provided. """
        for dependency in self._dependency_graph.pred[consumer].values():
            if not dependency['provided']:
                return False
        return True

    def get_dependencies_of(self, dependency):
        dependency = self.as_dependency(dependency)
        return self._dependency_graph.pred[dependency].keys()

    def resolve_initial_states(self):
        """ Resolve the initial states of any unresolved nodes in the dependency graph.

        .. note::

            The resolves are done in topological order --- a consumer can assume it's dependencies
            have been resolved before it is attempted resolved.

            Since resolving a `ResourceDependency` may result in new dependencies being added to the
            dependency graph, this function keeps resolving any unresolved dependencies until there
            are no unresolved nodes left in the dependency graph.
        """
        if not networkx.is_directed_acyclic_graph(self._dependency_graph):
            e_msg = 'A dependency graph cannot contain cycles.'
            detail = 'The following cycles were piped: \n\n %s'

            cycles = networkx.simple_cycles(self._dependency_graph)
            formatted_cycles = list()
            for cycle in cycles:
                formatted_cycles.append(' -> '.join([repr(dep) for dep in cycle]))

            detail = detail % ('\n'.join(formatted_cycles))
            raise exceptions.CircularDependencyGraph(e_msg, detail)

        while True:
            topological_sort = networkx.topological_sort(self._dependency_graph)

            unresolved_dependencies = [dependency for dependency in topological_sort if not self._dependency_graph.node[dependency]['resolved']]
            if not unresolved_dependencies:
                break

            for dependency in unresolved_dependencies:
                node_data = self._dependency_graph.node[dependency]
                if node_data.get('resolved', False):
                    continue
                node_data['resolved'] = True
                dependency.resolve_initial_state()

    def get_dot(self):
        """ Return a simple GraphViz-representation of the
        dependencies and their availability. """
        edges = []
        nodes_and_labels = set()
        for a, b, data in self._dependency_graph.edges(data=True):
            a_label = getattr(a, 'name', repr(a))
            b_label = getattr(b, 'name', repr(b))

            nodes_and_labels.add((a, repr(a), a_label))
            nodes_and_labels.add((b, repr(b), b_label))

            style = 'dotted'
            color = 'red'
            if data['provided']:
                style = 'solid'
                color = 'green'

            edges.append('"%s" -> "%s" [color=%s, style=%s];' % (repr(a), repr(b), color, style))

        for node, node_id, label in nodes_and_labels:
            node_data = self._dependency_graph.node[node]
            color = 'red'

            if node_data['ready']:
                color = 'green'

            shape = 'doublecircle'
            if node_data['resolved']:
                shape = 'ellipse'

            edges.append('"%s" [label="%s", shape=%s, color=%s];\n' % (node_id, label, shape, color))
        return 'digraph G { rankdir=BT; %s }' % '\n'.join(sorted(edges))

    def create_dependency_map(self, consumer, **initial_dependencies):
        """ Creates a :class:`DependencyMap` which is bound to this manager.

        :param initial_dependencies: dependencies that will be initially added to the map.
        :rtype: `DependencyMap`
        """
        map = DependencyMap(consumer, manager=self)

        for key, dependency_configuration in initial_dependencies.items():
            map[key] = dependency_configuration

        self.add_dependency(consumer, map)
        return map


class DependencyMap(InstanceDependency):
    """ Dict-like that proxies dependencies.

    The result of :func:`DependencyManager.create_dependency_map` is an
    instance of this class. The depended-on resources can be accessed
    as attributes/items in the dependency map, assuming they are
    available. If the accessed dependency is not provided, an
    :exc:`exceptions.UnsatisfiedDependencyError` is raised.
    """

    def __init__(self, consumer, *a, **kw):
        super(DependencyMap, self).__init__(instance=self, *a, **kw)
        
        self.consumer = consumer

        self._resource_by_key = dict()
        self._dependency_by_key = dict()

        # store the key for each dependency so we can look up the key for each
        # dependency object.
        self._key_by_dependency = dict()

        self.on_dependency_ready += self._dependency_ready
        self.on_dependency_lost += self._dependency_lost

    def _consider_cascading_ready(self):
        if len(self._dependency_by_key) == len(self._resource_by_key):
            super(DependencyMap, self)._consider_cascading_ready()

    def add_dependency(self, key, maybe_dependency):
        """ Adds a dependency entry to this `DependencyMap`.

        :param key: The key to use in the map.
        :param maybe_dependency: The dependency specification.
        """
        self._fail_if_dependency_already_exists(key)

        dependency = self.manager.as_dependency(maybe_dependency)

        # we need to configure these dicts before we add the dependency, as
        # it may be immediately available
        self._dependency_by_key[key] = dependency
        self._key_by_dependency[dependency] = key

        self.manager.add_dependency(self, dependency)
        
        return dependency

    def _fail_if_dependency_already_exists(self, key):
        if key in self._dependency_by_key:
            msg = 'Key already exists: %r.' % key
            detail = 'A DependencyMap does not support having multiple dependencies with the same key.'
            hint = ('Use DependencyMap.remove_dependency to remove the existing dependency'
                    'before adding a new dependency with the same key.')
            raise exceptions.KeyAlreadyExists(msg, detail, hint)

    def remove_dependency(self, key):
        """ Removes a dependency entry.

        :param key: The key identifying the dependency to remove.
        """
        dependency = self._dependency_by_key[key]
        self.manager.remove_dependency(self, dependency)

        del self._dependency_by_key[key]
        del self._resource_by_key[key]
        del self._key_by_dependency[dependency]

        return dependency

    def wait_for_resource(self, key, timeout=None):
        """ Return a deferred that fires with a resource when it becomes
        available.

        :param timeout: Time in seconds before a :class:`~piped.exceptions.TimeoutError`
            should be raised if the resource is not ready. Defaults to ``None``, which
            means no timeout.
        """
        return self._dependency_by_key[key].wait_for_resource(timeout=timeout)

    def _dependency_ready(self, dependency):
        key = self._key_by_dependency[dependency]
        self._resource_by_key[key] = dependency.get_resource()
        self._consider_cascading_ready()

    def _dependency_lost(self, dependency, reason):
        key = self._key_by_dependency[dependency]
        self._resource_by_key.pop(key, None)

    def __getattr__(self, key):
        if key not in self._dependency_by_key:
            raise AttributeError(key)

        try:
            return self._resource_by_key[key]
        except KeyError:
            raise exceptions.UnsatisfiedDependencyError('Unsatisfied dependency: %s' % str(key))

    __getitem__ = __getattr__

    def __setitem__(self, key, value):
        self.add_dependency(key, value)

    def __delattr__(self, key):
        self.remove_dependency(key)

    def is_available(self, key):
        """ Returns whether the requested resource is available. """
        return key in self._resource_by_key

    def __contains__(self, key):
        """ Returns whether the requested dependency exists in this map.

        To check whether the resource is available, use :meth:`.is_available`.
        """
        return key in self._dependency_by_key

    def __repr__(self):
        lost = dict([key, dep] for key, dep in  self._dependency_by_key.items() if key not in self._resource_by_key)
        return 'DependencyMap(ready=%s, lost=%s)'%(self._resource_by_key, lost)

    def __iter__(self):
        return iter(self._resource_by_key)

    def items(self):
        """ Returns a list of (key, resource) tuples for all *available* resources. """
        return self._resource_by_key.items()

    def keys(self):
        """ Returns a list of keys for the *available* resources. """
        return self._resource_by_key.keys()
