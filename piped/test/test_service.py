# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
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
        self.manager = StubServicePluginManager()

    def configure_and_get_service(self):
        self.manager.configure(self.runtime_environment)
        return self.manager.services[0]

    def test_making_service(self):
        self.manager.configure(self.runtime_environment)
        self.assertEquals(len(self.manager.services), 1)

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
