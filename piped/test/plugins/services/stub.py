from twisted.internet import defer
from zope import interface

from piped import service, util


class StubService(service.PipedService):
    interface.classProvides(service.IPipedService)

    def __init__(self):
        super(StubService, self).__init__()
        self.is_configured = False
        self.run_invoked = 0
        self.was_cancelled = False

    def configure(self, runtime_environment):
        self.is_configured = True

    def run(self):
        self.run_invoked += 1

    def cancel(self):
        super(StubService, self).cancel()
        self.was_cancelled = True


class StubDisabledService(service.PipedService):
    interface.classProvides(service.IPipedService)

    def is_enabled(self):
        return False


class StubDependencyService(service.PipedDependencyService):
    interface.classProvides(service.IPipedService)

    def __init__(self):
        super(StubDependencyService, self).__init__()
        self.running_with_dependencies = 0

    @defer.inlineCallbacks
    def run_with_dependencies(self):
        try:
            self.running_with_dependencies += 1

            while self.running:
                yield self.cancellable(util.wait(1))
        except Exception as e:
            self.running_with_dependencies -= 1
            raise