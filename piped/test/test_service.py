# Copyright (c) 2010-2012, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import weakref

from twisted.internet import defer, reactor
from twisted.trial import unittest

from piped import processing, service
from piped.test.plugins import services as service_plugins
from piped.test.plugins.services import stub


class StubServicePluginManager(service.ServicePluginManager):
    plugin_packages = [service_plugins]


class ServicePluginManagerTest(unittest.TestCase):
    def setUp(self):
        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()
        self.manager = StubServicePluginManager()

    def configure_and_get_service(self, service_class=stub.StubService):
        self.manager.configure(self.runtime_environment)
        for service in self.manager.services:
            if isinstance(service, service_class):
                return service

    def test_making_service(self):
        self.manager.configure(self.runtime_environment)
        # the disabled service should not be started
        self.assertEquals(len(self.manager.services), 2)
        # .. but its important for the correctness of this test that it is found:
        self.assertIn(stub.StubDisabledService, self.manager._plugins)

    def test_configuring_service(self):
        stub_service = self.configure_and_get_service()
        self.assertTrue(stub_service.is_configured)

    def test_service_not_started_until_manager_starts(self):
        stub_service = self.configure_and_get_service()
        self.assertFalse(stub_service.running)

    def test_service_starts_when_manager_starts(self):
        stub_service = self.configure_and_get_service()
        self.manager.startService()
        self.assertTrue(stub_service.running)

    def test_service_stops_when_manager_stops(self):
        stub_service = self.configure_and_get_service()
        self.manager.startService()
        self.assertTrue(stub_service.running)
        self.manager.stopService()
        self.assertFalse(stub_service.running)


class PipedServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = stub.StubService()

    def test_invokes_run_once_when_started(self):
        self.assertEquals(self.service.run_invoked, 0)
        self.service.startService()
        self.assertEquals(self.service.run_invoked, 1)
        self.service.startService()
        # Should only be invoked once
        self.assertEquals(self.service.run_invoked, 1)

    def test_cancels_when_stopped(self):
        self.service.startService()
        self.service.stopService()
        self.assertTrue(self.service.was_cancelled)

    @defer.inlineCallbacks
    def test_cancels_when_stopped(self):
        will_not_callback = defer.Deferred()
        self.service.startService()

        reactor.callLater(0, self.service.stopService)
        self.assertFalse(will_not_callback.called)

        try:
            yield self.service.cancellable(will_not_callback)
            self.fail('Expected error')
        except defer.CancelledError:
            pass

    def test_not_holding_references_to_callbacked_cancellables(self):
        d = defer.Deferred()
        ref = weakref.ref(d)
        self.service.cancellable(d)
        d.callback(42)
        del d
        self.assertIsNone(ref())


class PipedDependencyServiceTest(unittest.TestCase):

    def setUp(self):
        self.service = stub.StubDependencyService()

        self.runtime_environment = processing.RuntimeEnvironment()
        self.runtime_environment.configure()
        self.dependency_manager = self.runtime_environment.dependency_manager

        self.service.configure(self.runtime_environment)

    def tearDown(self):
        return self.service.stopService()

    def test_runs_only_when_ready(self):
        self.assertEquals(self.service.running_with_dependencies, 0)
        self.service.startService()
        # since the dependency system has not marked the service as ready, it should not be running:
        self.assertEquals(self.service.running, True)
        self.assertEquals(self.service.running_with_dependencies, 0)

        self.dependency_manager.resolve_initial_states()
        self.assertEquals(self.service.running_with_dependencies, 1)

    def test_runs_only_once(self):
        self.service.startService()
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(self.service.running_with_dependencies, 1)

        self.service.startService()
        self.assertEquals(self.service.running_with_dependencies, 1)

        self.service.self_dependency.fire_on_ready()
        self.assertEquals(self.service.running_with_dependencies, 1)

    def test_cancels_when_stopped(self):
        self.service.startService()
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(self.service.running_with_dependencies, 1)

        self.service.stopService()
        self.assertEquals(self.service.running_with_dependencies, 0)

    def test_cancels_when_dependency_lost(self):
        self.service.startService()
        self.dependency_manager.resolve_initial_states()
        self.assertEquals(self.service.running_with_dependencies, 1)

        self.service.self_dependency.fire_on_lost('reason')
        self.assertEquals(self.service.running_with_dependencies, 0)
