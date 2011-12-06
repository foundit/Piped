import os

from zope import interface
from twisted.internet import defer, utils

from piped.plugins.status_testing import statustest, processors
from piped import processing

class TestTwitterProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-twitter'

    class TestTwitter(statustest.StatusTestCase):
        timeout = 2

        def setUp(self, auth):
            # if the auth is overridden in our test configuration, pass it on to the process.
            self.auth = auth
            self.auth_override = []
            if self.auth:
                self.auth_override = ['-O', 'twitter.my_account.auth: {username: %(username)s, password: %(password)s}' % self.auth]
        
        @defer.inlineCallbacks
        def statustest_rate_limit_status(self):
            output = yield utils.getProcessOutput('piped', args=['-nc', 'twitter.yaml'] + self.auth_override, env=os.environ)
            self.assertIn('Current rate limit status:', output)

    def configure(self, runtime_environment):
        self.auth = runtime_environment.get_configuration_value('secrets.twitter.auth', None)

    def get_namespace(self, baton):
        return dict(auth=self.auth)