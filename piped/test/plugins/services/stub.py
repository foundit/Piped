from zope import interface

from piped import service


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
