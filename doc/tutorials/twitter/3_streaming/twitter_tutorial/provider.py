import tweepy
from zope import interface
from twisted.application import service
from twisted.internet import defer, threads, reactor

from piped import exceptions, log, resource, util


class MyTwitterProvider(service.MultiService):
    # state that we are a resource provider, so that the piped plugin system finds us
    interface.classProvides(resource.IResourceProvider)

    def __init__(self):
        super(MyTwitterProvider, self).__init__()

        # we store the apis by the account name since we might have
        # multiple consumers of the same api.
        self._api_by_name = dict()

    def configure(self, runtime_environment):
        self.setServiceParent(runtime_environment.application)
        self.twitter_configs = runtime_environment.get_configuration_value('twitter', dict())

        for account_name, account_config in self.twitter_configs.items():

            auth = self._get_auth(account_name, account_config['auth'])
            self._api_by_name[account_name] = tweepy.API(auth)

            # tell the resource manager that we can provide the named twitter accounts
            runtime_environment.resource_manager.register('twitter.%s' % account_name, provider=self)

            # create the configured streams:
            for stream_name, stream_config in account_config.get('streams', dict()).items():
                name = '%s.%s' % (account_name, stream_name)
                stream_listener = TwitterStreamListener(name, auth, **stream_config)
                stream_listener.configure(runtime_environment)

                # the service should be started/stopped when we are:
                stream_listener.setServiceParent(self)

    def _get_auth(self, account_name, auth_config):
        """ Returns an auth handler for the given account. """

        if 'username' in auth_config:
            return tweepy.BasicAuthHandler(**auth_config)

        access_token = auth_config.pop('access_token', None)

        auth = tweepy.OAuthHandler(**auth_config)

        if not access_token:
            print 'The account %r is missing the access_key/access_secret.'%account_name
            print 'Go the the following URL to authorize: %s,'%auth.get_authorization_url()
            print 'then paste the PID into this window and press enter.'

            verifier = raw_input('PID: ').strip()
            auth.get_access_token(verifier)

            print 'Add the following keys to the auth configuration %r:'%'twitter.%s.auth'%account_name
            print 'access_token:'
            print '    key: %s' % auth.access_token.key
            print '    secret: %s' % auth.access_token.secret
        else:
            auth.set_access_token(**access_token)

        return auth

    def add_consumer(self, resource_dependency):
        # since we registered for 'twitter.<account_name>', we can find the account_name requested by splitting:
        twitter, account_name = resource_dependency.provider.split('.')

        resource_dependency.on_resource_ready(self._api_by_name[account_name])


class TwitterStreamListener(tweepy.StreamListener, service.MultiService):
    """ A custom stream listener that feeds statuses into pipelines. """

    def __init__(self, name, auth, processor, method, **method_kwargs):
        """
        :param auth: A tweepy AuthHandler.
        :param processor: The processor name or config.
        :param method: The stream method to use.
        :param method_kwargs: Additional keyword arguments to pass to the method.
        """
        super(TwitterStreamListener, self).__init__()

        self.name = name

        # by setting ourselves as the streams service parent, it will be started and
        # stopped when we are.
        self.stream = TwitterStream(auth, listener=self)
        self.stream.setServiceParent(self)

        # Get and call the stream method. Since we've overridden ``_start``, no requests
        # will be performed before the service is started.
        self.method = getattr(self.stream, method)
        self.method(**method_kwargs)

        # create the dependency config for the processor, and allow the user to use a simple string as the
        # processor name as a shorthand.
        self.processor_config = dict(provider=processor) if isinstance(processor, basestring) else processor

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        # Create a dependency to the pipeline that we will use to process the statuses
        self.pipeline_dependency = dm.add_dependency(self, self.processor_config)

    def on_status(self, status):
        # on_status is called from outside the twisted reactor mainthread, and we want to process the
        # status inside the reactor:
        reactor.callFromThread(self._process_status, status)

    def on_error(self, status_code):
        reactor.callFromThread(log.error, 'Twitter api error (%s): %s' % (self.name, status_code))
        return False

    @defer.inlineCallbacks
    def _process_status(self, status):
        # We've received a status object from our stream and are now in the main thread of the process.
        # create the baton that will be processed in the pipeline.
        baton = dict(status=status)

        # wait for the pipeline to become available, then process the baton
        pipeline = yield self.pipeline_dependency.wait_for_resource()
        yield pipeline(baton)


class TwitterStream(tweepy.Stream, service.Service):
    """ Adapts a tweepy.Stream to a twisted Service. """

    def _start(self, async):
        """ We handle running the request in startService instead, forcing async=True """

    @defer.inlineCallbacks
    def startService(self):
        """ Start the reqest in the reactors thread pool. """
        self.running = True
        self.running_service = True

        while self.running_service:
            try:
                yield threads.deferToThread(self._run)
            except Exception as e:
                log.error()

    def stopService(self):
        self.running = False
        self.running_service = False
