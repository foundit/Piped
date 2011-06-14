import tweepy
from zope import interface
from twisted.application import service
from twisted.internet import defer, threads, reactor

from piped import exceptions, log, resource, util


class MyTwitterProvider(object):
    # state that we are a resource provider, so that the piped plugin system finds us
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        # we store the apis by the account name since we might have
        # multiple consumers of the same api.
        self._api_by_name = dict()

    def configure(self, runtime_environment):
        # look up the twitter account configurations:
        self.twitter_configs = runtime_environment.get_configuration_value('twitter', dict())

        for account_name, account_config in self.twitter_configs.items():

            auth = self._get_auth(account_name, account_config['auth'])
            self._api_by_name[account_name] = tweepy.API(auth)

            # tell the resource manager that we can provide the named twitter accounts
            runtime_environment.resource_manager.register('twitter.%s' % account_name, provider=self)

    def _get_auth(self, account_name, auth_config):
        """ Returns an auth handler for the given account. """

        if 'username' in auth_config:
            return tweepy.BasicAuthHandler(**auth_config)

        access_token = auth_config.pop('access_token', None)

        auth = tweepy.OAuthHandler(**auth_config)

        if not access_token:
            print 'The account %r is missing the access_key/access_secret.' % account_name
            print 'Go the the following URL to authorize: %s,' % auth.get_authorization_url()
            print 'then paste the PID into this window and press enter.'

            verifier = raw_input('PID: ').strip()
            auth.get_access_token(verifier)

            print 'Add the following keys to the auth configuration %r:' % ('twitter.%s.auth' % account_name)
            print 'access_token:'
            print '    key: %s' % auth.access_token.key
            print '    secret: %s' % auth.access_token.secret
        else:
            auth.set_access_token(**access_token)

        return auth

    def add_consumer(self, resource_dependency):
        # since we registered for 'twitter.<account_name>', we can find the account_name requested by splitting:
        twitter, account_name = resource_dependency.provider.split('.')

        # give the tweepy API instance to the resource:
        resource_dependency.on_resource_ready(self._api_by_name[account_name])
