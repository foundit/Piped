import os
import subprocess

from zope import interface
from twisted.internet import defer, utils
from twisted.web import client

from piped.contrib.status_testing import statustest, processors
from piped import processing, util


class TestTwitterProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-twitter'

    class TestTwitter(statustest.StatusTestCase):
        timeout = 5

        def setUp(self, auth):
            # if the auth is overridden in our test configuration, pass it on to the process.
            self.auth = auth

            self.oauth_override = ['-O', 'twitter.my_account.auth: {consumer_key: %(consumer_key)s, consumer_secret: %(consumer_secret)s }' % self.auth]
            self.oauth_override += ['-O', 'twitter.my_account.auth.access_token: {key: %(key)s, secret: %(secret)s}' % self.auth['access_token']]

        @defer.inlineCallbacks
        def statustest_status_updates_received(self):
            sub = subprocess.Popen(args=['piped', '-nc', 'twitter.yaml']+self.oauth_override, stdout=subprocess.PIPE)

            # wait four seconds
            yield util.wait(4)

            sub.terminate()

            status_updates = [line for line in sub.stdout if 'status-update -> ' in line]
            self.assertNotEquals(status_updates, list())
            

    def configure(self, runtime_environment):
        self.auth = runtime_environment.get_configuration_value('secrets.twitter.auth', None)

    def get_namespace(self, baton):
        return dict(auth=self.auth)