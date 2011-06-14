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

            auth = tweepy.BasicAuthHandler(**account_config['auth'])
            self._api_by_name[account_name] = tweepy.API(auth)

            # tell the resource manager that we can provide the named twitter accounts
            runtime_environment.resource_manager.register('twitter.%s' % account_name, provider=self)

    def add_consumer(self, resource_dependency):
        # since we registered for 'twitter.<account_name>', we can find the account_name requested by splitting:
        twitter, account_name = resource_dependency.provider.split('.')

        # give the tweepy API instance to the resource:
        resource_dependency.on_resource_ready(self._api_by_name[account_name])
