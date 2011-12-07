import uuid
import os
import tweepy

from zope import interface
from twisted.internet import defer, utils
from twisted.web import client

from piped.plugins.status_testing import statustest, processors
from piped import processing, util


class TestTwitterProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-twitter'

    class TestTwitter(statustest.StatusTestCase):
        timeout = 10

        def setUp(self, auth):
            # if the auth is overridden in our test configuration, pass it on to the process.
            self.auth = auth

            self.oauth_override = ['-O', 'twitter.my_account.auth: {consumer_key: %(consumer_key)s, consumer_secret: %(consumer_secret)s }' % self.auth]
            self.oauth_override += ['-O', 'twitter.my_account.auth.access_token: {key: %(key)s, secret: %(secret)s}' % self.auth['access_token']]

        @defer.inlineCallbacks
        def statustest_status_updated(self):
            unique_status = uuid.uuid4().hex
            generate_override = ['-O', 'pipelines.generate-status:[{eval-lambda: {output_path: status, lambda: "baton: \'%s\'"}}]'%unique_status]

            yield utils.getProcessOutput('piped', ['-nc', 'twitter.yaml'] + self.oauth_override + generate_override, env=os.environ)

            # give twitter a second to propagate our updated status
            yield util.wait(1)

            api = tweepy.API()
            user = api.get_user(self.auth['username'])
            user = api.get_user(self.auth['username'])
            self.assertEquals(user.status.text, unique_status)

    
    def configure(self, runtime_environment):
        self.auth = runtime_environment.get_configuration_value('secrets.twitter.auth', None)

    def get_namespace(self, baton):
        return dict(auth=self.auth)