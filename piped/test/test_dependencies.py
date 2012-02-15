# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import itertools

from twisted.internet import reactor, defer
from twisted.trial import unittest

from piped import util, dependencies, processing, exceptions


class DependencyManagerTest(unittest.TestCase):

    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

        self.resource_manager = self.runtime_environment.resource_manager

        self.dependency_manager = self.runtime_environment.dependency_manager

    def test_accessing_unprovided_resources_result_in_an_exception(self):
        consumer = object()
        resource_dependency = dependencies.ResourceDependency(provider='unavailable', manager=self.dependency_manager)
        dependency_map = self.dependency_manager.create_dependency_map(consumer, should_be_unavailable=resource_dependency)

        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependency_map, 'should_be_unavailable')
        self.assertRaises(AttributeError, getattr, dependency_map, 'nonexisting')

    def test_dependency_map_in(self):
        consumer = object()
        resource_dependency = dependencies.ResourceDependency(provider='unavailable', manager=self.dependency_manager)
        dependency_map = self.dependency_manager.create_dependency_map(consumer, should_be_unavailable=resource_dependency)

        self.assertIn('should_be_unavailable', dependency_map)
        self.assertNotIn('nonexisting', dependency_map)

    def test_accessing_provided_dependency(self):
        # Consumer wants fake_resource, but it's not directly
        # available as an instance. It's provided by a provider which
        # registers the resource as available with the path "fake"
        consumer = object()
        fake_resource = object()
        resource_path = 'fake'

        resource_dependency = dependencies.ResourceDependency(provider=resource_path, manager=self.dependency_manager)

        dependency_map = self.dependency_manager.create_dependency_map(consumer, directly_provided=resource_dependency)

        consumers = list()
        provider = util.AttributeDict(add_consumer=consumers.append)

        self.resource_manager.register(resource_path, provider)

        self.dependency_manager.resolve_initial_states()

        resource_dependency.on_resource_ready(fake_resource)
        self.assertEquals(dependency_map.directly_provided, fake_resource)

        # The providers add_dependency should be invoked with the resource dependency
        self.assertEquals(consumers, [resource_dependency])

    def test_getting_dependencies(self):
        foo = object()
        bar = object()
        baz = object()
        zip = object()

        self.dependency_manager.add_dependency(foo, bar)
        self.dependency_manager.add_dependency(baz, bar)
        self.dependency_manager.add_dependency(bar, zip)

        self.assertEquals(self.dependency_manager.get_dependencies_of(foo), [self.dependency_manager.as_dependency(bar)])
        self.assertEquals(self.dependency_manager.get_dependencies_of(zip), [])

    def test_bubbling_availability_for_trivial_dependencies(self):
        """
             A
            / \
           B   C
              / \
             D   E
        """
        dependencies = dict(
            A = self.dependency_manager.create_dependency_map('A', B='B', C='C'),
            C = self.dependency_manager.create_dependency_map('C', D='D', E='E')
        )

        is_ready = dict(A=False, B=False, C=False, D=False, E=False)
        for char in is_ready.keys():
            def enable(dependency, char=char):
                is_ready[char] = True

            self.dependency_manager.as_dependency(char).on_ready += enable

        self.dependency_manager.resolve_initial_states()

        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))
        self.assertEquals(dependencies['A'].B, 'B')
        self.assertEquals(dependencies['A'].C, 'C')
        self.assertEquals(dependencies['C'].D, 'D')
        self.assertEquals(dependencies['C'].E, 'E')

    def test_not_bubbling_when_not_supposed_to(self):
        """
             A
            / \
           B   C
              / \
             D   E
        """
        dependencies = dict(
            A=self.dependency_manager.create_dependency_map('A', B='B', C='C'),
            C=self.dependency_manager.create_dependency_map('C', D='D', E='E')
        )

        c_dependency = self.dependency_manager.as_dependency('C')
        # make sure C does not fire its on_ready when all its dependencies are ready
        c_dependency.cascade_ready = False

        dependencies_are_available = dict(A=False, C=False)
        is_ready = dict(A=False, B=False, C=False, D=False, E=False)
        for char in is_ready.keys():
            def update_availability(readied_dependency, char=char):
                char_dependency = self.dependency_manager.as_dependency(char)
                if self.dependency_manager.has_all_dependencies_provided(char_dependency):
                    dependencies_are_available[char] = True

            def enable_ready(dependency, char=char):
                is_ready[char] = True

            self.dependency_manager.as_dependency(char).on_dependency_ready += update_availability
            self.dependency_manager.as_dependency(char).on_ready += enable_ready

        self.dependency_manager.resolve_initial_states()

        # Note that C's dependencies are available, ...
        self.assertEquals(dependencies_are_available, dict(A=False, C=True))
        # ... but it is not automatically declared available due to cascade_read=False
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=True, E=True))
        self.assertEquals(dependencies['A'].B, 'B')
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependencies['A'], 'C')
        self.assertEquals(dependencies['C'].D, 'D')
        self.assertEquals(dependencies['C'].E, 'E')

        # Declare C as being ready
        c_dependency.fire_on_ready()

        # That should bubble to A as well.
        self.assertEquals(dependencies_are_available, dict(A=True, C=True))
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))
        # ... and C should be available to A.
        self.assertEquals(dependencies['A'].C, 'C')

    def test_bubbling_unavailability(self):
        """
             A
            / \
           B   C   F
              / \ /
             D   E
        """
        dependencies = dict(
            A=self.dependency_manager.create_dependency_map('A', B='B', C='C'),
            C=self.dependency_manager.create_dependency_map('C', D='D', E='E'),
            F=self.dependency_manager.create_dependency_map('F', E='E')
        )

        is_ready = dict(A=False, B=False, C=False, D=False, E=False, F=False)
        for char in is_ready.keys():
            def enable(dependency, char=char):
                is_ready[char] = True

            def disable(dependency, failure=None, char=char):
                is_ready[char] = False

            self.dependency_manager.as_dependency(char).on_ready += enable
            self.dependency_manager.as_dependency(char).on_lost += disable

        self.dependency_manager.resolve_initial_states()

        # All should be trivially up when starting out.
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, F=True))

        # Fail E
        e_dependency = self.dependency_manager.as_dependency('E')
        e_dependency.fire_on_lost('e failed somehow')

        # That should result in F, C and A having unsatisfied
        # dependencies.

        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=True, E=False, F=False))
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependencies['A'], 'C')
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependencies['C'], 'E')
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependencies['F'], 'E')
        self.assertEquals(dependencies['A'].B, 'B')
        self.assertEquals(dependencies['C'].D, 'D')

        # Bring E up again.
        e_dependency.fire_on_ready()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, F=True))
        self.assertEquals(dependencies['A'].C, 'C')
        self.assertEquals(dependencies['C'].E, 'E')
        self.assertEquals(dependencies['F'].E, 'E')

        # Fail B
        b_dependency = self.dependency_manager.as_dependency('B')
        b_dependency.fire_on_lost('b failed somehow')
        # Should take down A as well.
        self.assertEquals(is_ready, dict(A=False, B=False, C=True, D=True, E=True, F=True))
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependencies['A'], 'B')
        # Bring it up again
        b_dependency.fire_on_ready()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, F=True))
        self.assertEquals(dependencies['A'].B, 'B')
        self.assertEquals(dependencies['A'].C, 'C')
        self.assertEquals(dependencies['C'].D, 'D')
        self.assertEquals(dependencies['C'].E, 'E')
        self.assertEquals(dependencies['F'].E, 'E')

    def test_deferred_dependencies(self):
        """ Test propagating a dependency that is registered in a
        provider's add_consumer.

        I.e. in this case, we have the dependency chain A -> B ->
        C. First, A depends on B. Then B says it depends on C.

        The idea is that B has a dependency, but it should not be
        recorded before something depends on B.
        """
        class B:
            def configure(self, runtime_environment):
                self.runtime_environment = runtime_environment
                self.dependency_manager = self.runtime_environment.dependency_manager

                self.runtime_environment.resource_manager.register('B', provider=self)

            def add_consumer(self, consumer):
                self.dependency_manager.add_dependency(consumer, self.dependency_manager.as_dependency('C'))

        b_dependency = dependencies.ResourceDependency(provider='B', manager=self.dependency_manager)

        self.dependency_manager.add_dependency('A', b_dependency)

        a_dependency = self.dependency_manager.as_dependency('A')
        c_dependency = self.dependency_manager.as_dependency('C')

        B().configure(self.runtime_environment)

        self.dependency_manager.resolve_initial_states()

        # C -> B and B -> A
        self.assertEquals(set(self.dependency_manager._dependency_graph.edges()), set([(c_dependency, b_dependency), (b_dependency, a_dependency)]))

    def test_duplicate_named_dependencies(self):
        """ Test that duplicate names does not affect the dependency manager. """
        class C:
            def configure(self, runtime_environment):
                self.runtime_environment = runtime_environment
                self.runtime_environment.resource_manager.register('C', provider=self)

            def add_consumer(self, resource_dependency):
                # We use a tuple to ensure that the resource comes from this provider, and an object to
                # make every resource provided unique.
                resource = 'provided-by-c', object()
                resource_dependency.on_resource_ready(resource)

        class F:
            resource = 'provided-by-f'
            def configure(self, runtime_environment):
                self.runtime_environment = runtime_environment
                self.runtime_environment.resource_manager.register('F', provider=self)

            def add_consumer(self, resource_dependency):
                # This provider returns the same resource for all calls
                resource_dependency.on_resource_ready(self.resource)

        dep_a = dependencies.ResourceDependency(provider='C', manager=self.dependency_manager)
        dep_b = dependencies.ResourceDependency(provider='C', manager=self.dependency_manager)

        dep_d = dependencies.ResourceDependency(provider='F', manager=self.dependency_manager)
        dep_e = dependencies.ResourceDependency(provider='F', manager=self.dependency_manager)

        self.dependency_manager.create_dependency_map(self, a=dep_a, b=dep_b, d=dep_d, e=dep_e)

        C().configure(self.runtime_environment)
        F().configure(self.runtime_environment)

        self.dependency_manager.resolve_initial_states()

        # these should get a tuple of (an_unique_object, 'provided-by-c')
        self.assertTrue(dep_a.get_resource() != dep_b.get_resource())
        self.assertEquals(dep_a.get_resource()[0], 'provided-by-c')
        self.assertEquals(dep_b.get_resource()[0], 'provided-by-c')

        # f provides a simple string.
        self.assertEquals(dep_d.get_resource(), 'provided-by-f')
        self.assertEquals(dep_e.get_resource(), 'provided-by-f')

    def test_bubbling_availability_for_duplicate_named_dependencies(self):
        """
             A
            / \
           B   C   F
              / \ / \
             D   E   G

        G will never be provided, and this means F should not become available.
        """

        g_provider_consumers = list()

        class GProvider:
            def configure(self, runtime_environment):
                self.runtime_environment = runtime_environment
                self.runtime_environment.resource_manager.register('G', provider=self)

            def add_consumer(self, resource_dependency):
                g_provider_consumers.append(resource_dependency)

        GProvider().configure(self.runtime_environment)

        g_dependency = dependencies.ResourceDependency('G', manager=self.dependency_manager)

        deps = dict(
            A=self.dependency_manager.create_dependency_map('A', foo='B', bar='C'),
            C=self.dependency_manager.create_dependency_map('C', foo='D', bar='E'),
            F=self.dependency_manager.create_dependency_map('F', foo='E', bar=g_dependency),
        )

        is_ready = dict(A=False, B=False, C=False, D=False, E=False, F=False, G=False)
        for char in is_ready.keys():
            def enable(dependency, char=char):
                is_ready[char] = True

            self.dependency_manager.as_dependency(char).on_ready += enable

        self.dependency_manager.resolve_initial_states()

        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, F=False, G=False))
        self.assertEquals(deps['A'].foo, 'B')
        self.assertEquals(deps['A'].bar, 'C')
        self.assertEquals(deps['C'].foo, 'D')
        self.assertEquals(deps['C'].bar, 'E')
        self.assertEquals(deps['F'].foo, 'E')
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, deps['F'], 'bar')
        self.assertEquals(g_provider_consumers, [g_dependency])

    def test_multiple_resolves(self):
        """
              A-->X-->Y
            / \  / \
           B   C   |
              / \ /
             D   E # X, Y, D and E is added in later resolves
        """

        is_ready = dict()
        for char in 'A', 'B', 'C', 'D', 'E', 'X', 'Y':
            def enable(dependency, char=char):
                is_ready[char] = True
            def disable(dependency, reason, char=char):
                is_ready[char] = False

            self.dependency_manager.as_dependency(char).on_ready += enable
            self.dependency_manager.as_dependency(char).on_lost += disable

        dependency_map = self.dependency_manager.create_dependency_map('A', B='B', C='C')
        self.dependency_manager.resolve_initial_states()

        for instance in ('B', 'C'):
            self.assertEquals(dependency_map[instance], instance)

        # D and E doesn't exist yet.
        self.assertEquals(is_ready, dict(A=True, B=True, C=True))

        c_dependency = self.dependency_manager.as_dependency('C')
        d_dependency = self.dependency_manager.as_dependency('D')
        e_dependency = self.dependency_manager.as_dependency('E')

        self.dependency_manager.add_dependency(c_dependency, d_dependency)
        self.dependency_manager.add_dependency(c_dependency, e_dependency)

        # D and E has been added, but their initial states are yet to be resolved.
        self.assertEquals(is_ready, dict(A=True, B=True, C=True))

        self.dependency_manager.resolve_initial_states()

        # since instances start as ready, they are now marked as available in our dict
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))

        # and the dependency map still works
        for instance in ('B', 'C'):
            self.assertEquals(dependency_map[instance], instance)

        e_dependency.fire_on_lost('e got lost')

        # since C depend on E now, and E is lost, C is also lost
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=True, E=False))

        self.assertEquals(dependency_map['B'], 'B')
        self.assertRaises(exceptions.UnsatisfiedDependencyError, getattr, dependency_map, 'C')

        e_dependency.fire_on_ready()

        # create another dependency map, which will add some dependencies to the manager
        self.dependency_manager.create_dependency_map('X', A='A', C='C', E='E')

        # X should get the previous information replayed when we resolve the initial states
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, X=True))

        d_dependency.fire_on_lost('d got lost')
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=False, E=True, X=False))

        self.dependency_manager.add_dependency('Y', 'X')

        # Y is not provided (because it depends indirectly on d), neither before nor after the resolving
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=False, E=True, X=False))
        self.dependency_manager.resolve_initial_states()
        # ... but the resolve causes Y's on_lost to fire, so it turns up in our dictionary
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=False, E=True, X=False, Y=False))

        # if d comes up, everything else should too:
        d_dependency.fire_on_ready()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, X=True, Y=True))


    @defer.inlineCallbacks
    def test_multiple_resolves_with_resource_dependency(self):
        """
              A-->X-->Y
            / \  / \
           B   C   |  # C is a resource dependency
              / \ /
             D   E # X, Y, D and E is added in later resolves
        """

        is_ready = dict()

        for char in 'A', 'B', 'D', 'E', 'X', 'Y':
            self.dependency_manager.as_dependency(char).on_ready += lambda dep, char=char: is_ready.__setitem__(char, True)
            self.dependency_manager.as_dependency(char).on_lost += lambda dep, reason, char=char: is_ready.__setitem__(char, False)

        class CProvider(object):
            def configure(self, runtime_environment):
                self.runtime_environment = runtime_environment
                self.runtime_environment.resource_manager.register('C-Provider', provider=self)

            def add_consumer(self, resource_dependency):
                # declare the dependency as ready in the next reactor iteration
                reactor.callLater(0, resource_dependency.on_resource_ready, object())

        CProvider().configure(self.runtime_environment)

        # create a resource dependency on C, and add some hooks to update our is_ready dict
        c_dependency = dependencies.ResourceDependency('C-Provider', manager=self.dependency_manager)
        c_dependency.on_ready += lambda dep: is_ready.__setitem__('C', True)
        c_dependency.on_lost += lambda dep, reason: is_ready.__setitem__('C', False)

        # Add a dependency A, which requires B and C
        self.dependency_manager.create_dependency_map('A', B='B', C=c_dependency)

        self.dependency_manager.resolve_initial_states()
        # D and E doesn't exist yet, and C will become available in the next reactor iteration
        self.assertEquals(is_ready, dict(A=False, B=True, C=False))
        # .. so we wait one iteration
        yield util.wait(0)
        # and C should become available
        self.assertEquals(is_ready, dict(A=True, B=True, C=True))

        d_dependency = self.dependency_manager.as_dependency('D')
        e_dependency = self.dependency_manager.as_dependency('E')

        # add D -> C and E -> E
        self.dependency_manager.add_dependency(c_dependency, d_dependency)
        self.dependency_manager.add_dependency(c_dependency, e_dependency)

        # D and E has been added, but their initial states are yet to be resolved.
        self.assertEquals(is_ready, dict(A=True, B=True, C=True))

        self.dependency_manager.resolve_initial_states()
        # since instances start as ready, they are now marked as available in our dict
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))


        e_dependency.fire_on_lost('e got lost')
        # since C depend on E now, and E is lost, C is also lost
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=True, E=False))

        # we bring E back up again...
        e_dependency.fire_on_ready()
        # ... and add a new dependency, X, which requires A, C and E
        self.dependency_manager.create_dependency_map('X', A='A', C='C', E='E')

        # X should resolve its initial state to ready
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))
        # resolving initial states should not change anything in this case
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, X=True))

        # if D gets lost, a couple of things should be cascaded..
        d_dependency.fire_on_lost('d got lost')
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=False, E=True, X=False))

        # we add a dependency while a few dependencies are down:
        self.dependency_manager.create_dependency_map('Y', X='X')

        # Y is not provided, neither before nor after the resolving
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=False, E=True, X=False))
        self.dependency_manager.resolve_initial_states()
        # ... but the resolve causes Y's on_lost to fire, so it turns up in our dictionary
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=False, E=True, X=False, Y=False))

        # if d becomes ready, everything in our graph should be ready:

        d_dependency.fire_on_ready()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True, X=True, Y=True))

    def test_only_fires_on_changes(self):
        """
             A
            / \
           B   C
            \ / \
             D   E
        """
        counter = [0]
        a_ready_calls = list()
        a_lost_calls = list()

        def reset_calls():
            counter[0] = 0
            a_ready_calls[:] = list()
            a_lost_calls[:] = list()

        def assertCalls(ready=list(), lost=list(), msg=''):
            self.assertEquals(a_ready_calls, ready, msg)
            self.assertEquals(a_lost_calls, lost, msg='')

        def add_ready_call(dependency):
            counter[0] += 1
            self.assertEquals(dependency, a_dependency)
            a_ready_calls.append(counter[0])

        def add_lost_call(dependency, reason):
            counter[0] += 1
            self.assertEquals(dependency, a_dependency)
            a_lost_calls.append(counter[0])

        a_dependency = self.dependency_manager.as_dependency('A')
        a_dependency.on_ready += add_ready_call
        a_dependency.on_lost += add_lost_call

        b_dependency = self.dependency_manager.as_dependency('B')
        c_dependency = self.dependency_manager.as_dependency('C')
        d_dependency = self.dependency_manager.as_dependency('D')
        e_dependency = self.dependency_manager.as_dependency('E')

        self.dependency_manager.add_dependency(a_dependency, b_dependency)
        self.dependency_manager.add_dependency(a_dependency, c_dependency)
        self.dependency_manager.add_dependency(b_dependency, d_dependency)
        self.dependency_manager.add_dependency(c_dependency, d_dependency)
        self.dependency_manager.add_dependency(c_dependency, e_dependency)

        # assert that no calls have been made
        assertCalls()

        # resolving the initial states should result in one lost call (initial state), then one ready call
        self.dependency_manager.resolve_initial_states()
        assertCalls([1])
        reset_calls()

        # bringing down and up any of the dependencies only results in one  call
        for dependency in b_dependency, c_dependency, d_dependency, e_dependency:
            dependency.fire_on_lost('lost dependency')
            assertCalls(lost=[1])
            reset_calls()

            dependency.fire_on_ready()
            assertCalls(ready=[1])
            reset_calls()

        # bring down the dependencies in any order, and bring them up again - that should result in only
        # one on_lost and only one on_ready

        for dependencies in itertools.permutations((b_dependency, c_dependency, d_dependency, e_dependency)):
            for dependency in dependencies:
                dependency.fire_on_lost('lost dependency')
                # as soon as one dependency is lost, A should be lost. wrap this in an if to avoid asserting too many times
                if dependency == dependencies[0]:
                    assertCalls(lost=[1])


            for dependency in dependencies:
                dependency.fire_on_ready()

            assertCalls(lost=[1], ready=[2], msg=','.join([repr(dep) for dep in dependencies]))
            reset_calls()

    def test_on_dependency_only_fires_on_changes(self):
        """
             A
            / \
           B   C
            \ / \
             D   E
        """
        a_ready_calls = list()
        a_lost_calls = list()

        def reset_calls():
            a_ready_calls[:] = list()
            a_lost_calls[:] = list()

        def assertCalls(ready=list(), lost=list(), msg=''):
            self.assertEquals(sorted(a_ready_calls), sorted(ready), msg=msg)
            self.assertEquals(sorted(a_lost_calls), sorted(lost), msg=msg)

        def add_ready_call(dependency):
            a_ready_calls.append(dependency)

        def add_lost_call(dependency, reason):
            a_lost_calls.append(dependency)

        a_dependency = self.dependency_manager.as_dependency('A')
        a_dependency.on_dependency_ready += add_ready_call
        a_dependency.on_dependency_lost += add_lost_call

        b_dependency = self.dependency_manager.as_dependency('B')
        c_dependency = self.dependency_manager.as_dependency('C')
        d_dependency = self.dependency_manager.as_dependency('D')
        e_dependency = self.dependency_manager.as_dependency('E')

        self.dependency_manager.add_dependency(a_dependency, b_dependency)
        self.dependency_manager.add_dependency(a_dependency, c_dependency)
        self.dependency_manager.add_dependency(b_dependency, d_dependency)
        self.dependency_manager.add_dependency(c_dependency, d_dependency)
        self.dependency_manager.add_dependency(c_dependency, e_dependency)

        # assert that no calls have been made
        assertCalls()

        # resolving the initial states should result in one lost call, then one ready call for each dependency
        self.dependency_manager.resolve_initial_states()
        # The dependencies will be made available to A:
        assertCalls([b_dependency, c_dependency])
        reset_calls()

        # a change in a direct dependency should result in a single lost call
        for direct_dependency in b_dependency, c_dependency:
            direct_dependency.fire_on_lost('lost dependency')
            assertCalls(lost=[direct_dependency])
            reset_calls()

            direct_dependency.fire_on_ready()
            assertCalls(ready=[direct_dependency])
            reset_calls()

        # bringing down D should result in both B and C going down
        d_dependency.fire_on_lost('lost dependency')
        assertCalls(lost=[b_dependency, c_dependency])
        reset_calls()

        # if E goes up or down, nothing should change for A
        e_dependency.fire_on_lost('lost dependency')
        e_dependency.fire_on_ready()
        assertCalls()

        # when d comes up again, both b and c should become ready
        d_dependency.fire_on_ready()
        assertCalls(ready=[b_dependency, c_dependency])
        reset_calls()

        # bring down the dependencies in any order, and bring them up again - that should result in only
        # one on_lost and only one on_ready per dependency
        for dependencies in itertools.permutations((b_dependency, c_dependency, d_dependency, e_dependency)):
            for dependency in dependencies:
                # since lost-events cascade, and we're triggering the on_lost manually, we have to check
                # if the dependency already is lost, as otherwise it could cause one dependency to be on_lost twice
                # (once by the cascade, then later again by this call)
                if dependency.is_ready:
                    dependency.fire_on_lost('lost dependency')
                # as soon as one dependency is lost, one of As dependencies should be lost
                # wrap this in an if to avoid asserting too many times
                if dependency == dependencies[0]:
                    self.assertTrue(len(a_lost_calls) >= 1)

            for dependency in dependencies:
                # see the above reasoning about checking .ready
                if not dependency.is_ready:
                    dependency.fire_on_ready()

            assertCalls(lost=[b_dependency, c_dependency], ready=[b_dependency, c_dependency], msg=','.join([repr(dep) for dep in dependencies]))
            reset_calls()

    def test_rewriting_dependencies(self):
        ensure = self.dependency_manager.as_dependency
        # instances should be cached:
        self.assertIdentical(ensure('A'), ensure('A'))
        self.assertIsInstance(ensure('A'), dependencies.InstanceDependency)

        # dictionaries should be considered resource dependencies
        self.assertIsInstance(ensure(dict(provider='some provider string')), dependencies.ResourceDependency)
        # but they are not cached:
        self.assertNotEquals(ensure(dict(provider='A')), ensure(dict(provider='A')))
        # and without a provider key, they're invalid:
        self.assertRaises(exceptions.ResourceError, ensure, dict(foo='bar'))

        # things that already are dependencies should be untouched:
        dependency = dependencies.Dependency()
        self.assertIdentical(dependency, ensure(dependency))

    def test_cyclic_raises(self):
        # A -> B
        self.dependency_manager.add_dependency('A', 'B')
        self.dependency_manager.add_dependency('B', 'A')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)
        self.dependency_manager.remove_dependency('B', 'A')

        # self-loops are also cycles
        self.dependency_manager.add_dependency('B', 'B')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)
        self.dependency_manager.remove_dependency('B', 'B')

        self.dependency_manager.add_dependency('A', 'A')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)
        self.dependency_manager.remove_dependency('A', 'A')

        # even if there is a node between them
        # A -> B -> C
        self.dependency_manager.add_dependency('B', 'C')

        self.dependency_manager.add_dependency('C', 'A')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)
        self.dependency_manager.remove_dependency('C', 'A')

        # or two
        # A -> B -> C -> D
        self.dependency_manager.add_dependency('C', 'D')

        self.dependency_manager.add_dependency('D', 'C')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)
        self.dependency_manager.remove_dependency('D', 'C')

        self.dependency_manager.add_dependency('D', 'B')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)
        self.dependency_manager.remove_dependency('D', 'B')

        self.dependency_manager.add_dependency('B', 'A')
        self.dependency_manager.add_dependency('D', 'A')
        self.assertRaises(exceptions.CircularDependencyGraph, self.dependency_manager.resolve_initial_states)


    def test_adding_existing_dependency_raises(self):
        self.dependency_manager.add_dependency('A', 'B')
        self.assertRaises(exceptions.AlreadyExistingDependency, self.dependency_manager.add_dependency, 'A', 'B')

    def test_dependencies_updated(self):
        """
            A
           / \
          B   C
             / ~ E # gets added later
            D
        """
        is_ready = dict()

        for char in 'A', 'B', 'C', 'D', 'E':
            self.dependency_manager.as_dependency(char).on_ready += lambda dep, char=char: is_ready.__setitem__(char, True)
            self.dependency_manager.as_dependency(char).on_lost += lambda dep, reason, char=char: is_ready.__setitem__(char, False)

        self.dependency_manager.add_dependency('A', 'B')
        self.dependency_manager.add_dependency('A', 'C')
        self.dependency_manager.add_dependency('C', 'D')

        self.assertEquals(is_ready, dict())
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True))

        # C now should depend on E
        self.dependency_manager.add_dependency('C', 'E')
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True))
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))

        # if E goes down, it should cascade to A and C
        e_dependency = self.dependency_manager.as_dependency('E')
        e_dependency.fire_on_lost('e lost')
        self.assertEquals(is_ready, dict(A=False, B=True, C=False, D=True, E=False))

        # but if C no longer depends on E, it should become ready again
        self.dependency_manager.remove_dependency('C', 'E')
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=False))

        # if E goes up, everything should be up
        e_dependency.fire_on_ready()
        self.assertEquals(is_ready, dict(A=True, B=True, C=True, D=True, E=True))


class DependencyMapTest(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()

        self.resource_manager = self.runtime_environment.resource_manager

        self.dependency_manager = self.runtime_environment.dependency_manager

    def _create_map(self, consumer=None):
        if not consumer:
            consumer = self
        return dependencies.DependencyMap(consumer, manager=self.dependency_manager)

    def _create_provider(self, add_consumer_callback, *resource_paths):
        class Provider:
            def add_consumer(self, resource_dependency):
                add_consumer_callback(resource_dependency)
        p = Provider()
        for resource_path in resource_paths:
            self.resource_manager.register(resource_path, p)


    def test_simple_dependency_map(self):
        self._create_provider(lambda dep: dep.on_resource_ready('foo-resource'), 'foo')

        map = self._create_map()
        map['foo'] = dependencies.ResourceDependency(provider='foo')
        map['bar'] = 'this will become an instance dependency'

        self.dependency_manager.resolve_initial_states()

        self.assertEquals(map['foo'], 'foo-resource')
        self.assertEquals(map['bar'], 'this will become an instance dependency')
        self.assertRaises(exceptions.KeyAlreadyExists, map.add_dependency, 'foo', 'anything')

        map.remove_dependency('foo')
        map.add_dependency('foo', dict(provider='foo'))

        # since this introduced a new dependency to the dependency manager, we need to resolve it before
        # it becomes available:
        self.assertRaises(exceptions.UnsatisfiedDependencyError, map.__getitem__, 'foo')
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(map['foo'], 'foo-resource')

    def test_resource_changes_updates_the_map(self):
        resource_dependencies = list()
        self._create_provider(lambda dep: resource_dependencies.append(dep), 'foo')

        foo_resource = dependencies.ResourceDependency(provider='foo')

        map = self._create_map()
        map['foo'] = foo_resource

        self.dependency_manager.resolve_initial_states()

        self.assertEquals(len(resource_dependencies), 1)
        self.assertEquals(resource_dependencies[0], foo_resource)
        self.assertRaises(exceptions.UnsatisfiedDependencyError, map.__getitem__, 'foo')

        d = map.wait_for_resource('foo')
        self.assertEquals(d.called, False)

        res = 'foo resource is ready'
        foo_resource.on_resource_ready(res)
        self.assertEquals(map['foo'], res)

        self.assertEquals(d.called, True)
        self.assertEquals(d.result, res)

        # if the resource is available immediately
        d = map.wait_for_resource('foo')
        self.assertEquals(d.called, True)
        self.assertEquals(d.result, res)

        # if the resource is lost, it should unavailable, but still possible to wait for
        foo_resource.on_resource_lost('resource lost')

        d = map.wait_for_resource('foo')
        self.assertRaises(exceptions.UnsatisfiedDependencyError, map.__getitem__, 'foo')
        self.assertEquals(d.called, False)

        # when the resource becomes ready, the deferred should be callbacked.
        foo_resource.on_resource_ready(res)
        self.assertEquals(d.called, True)
        self.assertEquals(d.result, res)


__doctests__ = [dependencies]