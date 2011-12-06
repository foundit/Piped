import os
import subprocess

from zope import interface
from twisted.internet import defer, utils
from twisted.web import client

from piped.plugins.status_testing import statustest, processors
from piped import processing


class TestTwitterProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-twitter'

    class TestTwitter(statustest.StatusTestCase):
        timeout = 4

        def setUp(self, auth):
            # if the auth is overridden in our test configuration, pass it on to the process.
            self.auth = auth

            self.auth_override = ['-O', 'twitter.my_account.auth: {username: %(username)s, password: %(password)s}' % self.auth]

            self.oauth_without_access_override = ['-O', 'twitter.my_account.auth: {consumer_key: %(consumer_key)s, consumer_secret: %(consumer_secret)s}' % self.auth]
            
            self.oauth_override = ['-O', 'twitter.my_account.auth: {consumer_key: %(consumer_key)s, consumer_secret: %(consumer_secret)s }' % self.auth]
            self.oauth_override += ['-O', 'twitter.my_account.auth.access_token: {key: %(key)s, secret: %(secret)s}' % self.auth['access_token']]
        
        @defer.inlineCallbacks
        def statustest_basic_auth(self):
            output = yield utils.getProcessOutput('piped', args=['-nc', 'twitter.yaml', '-p', 'basic.pid'] + self.auth_override, env=os.environ)
            self.assertIn('Current rate limit status:', output)

        @defer.inlineCallbacks
        def statustest_oauth(self):
            output = yield utils.getProcessOutput('piped', args=['-nc', 'twitter.yaml', '-p', 'oauth.pid'] + self.oauth_override, env=os.environ)
            self.assertIn('Current rate limit status:', output)

        @defer.inlineCallbacks
        def statustest_oauth_dance(self):
            sub = subprocess.Popen(args=['piped', '-nc', 'twitter.yaml', '-p', 'oauth_dance.pid']+self.oauth_without_access_override, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)

            while True:
                line = sub.stdout.readline()
                
                if 'Go the the following URL to authorize' in line:
                    oauth_token = line.strip(' ,\n').rsplit('=', 1)[-1]

                if 'paste the PID into this window and press enter.' in line:
                    url = 'https://api.twitter.com/oauth/authorize'
                    postdata = 'oauth_token=%s&session[username_or_email]=%s&session[password]=%s&allow=Authorize+app' % (oauth_token, self.auth['username'], self.auth['password'])

                    page = yield client.getPage(url, method='POST', postdata=postdata)

                    for line in page.split('\n'):
                        if '<code>' in line:
                            pin = line.split('<code>')[-1].split('</code>')[0]

                    sub.stdin.write('%s\n' % pin)

                if 'Unauthorized' in line:
                    self.fail('Could not authorize.')

                if 'Current rate limit status' in line:
                    break
            
            sub.terminate()
            

    def configure(self, runtime_environment):
        self.auth = runtime_environment.get_configuration_value('secrets.twitter.auth', None)

    def get_namespace(self, baton):
        return dict(auth=self.auth)