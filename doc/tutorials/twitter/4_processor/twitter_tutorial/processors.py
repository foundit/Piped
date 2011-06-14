from tweepy import error
from zope import interface
from twisted.internet import defer

from piped import util, processing, log
from piped.processors import base


class UpdateStatus(base.Processor):
    """ Update the Twitter status of an account. """
    # Make instances of this processor available for use in the pipelines:
    interface.classProvides(processing.IProcessor)

    # every processor needs a name, which is used in the configuration:
    name = 'update-twitter-status'

    def __init__(self, account, status_path='status', **kw):
        """
        :param account: The name of the Twitter account we should use.
        :param status_path: The path to the status message inside the baton.
        """
        super(UpdateStatus, self).__init__(**kw)
        
        self.account_name = account
        self.status_path = status_path

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        # add a dependency to the twitter api of the specified account.
        self.api_dependency = dm.add_dependency(self, dict(provider='twitter.%s'%self.account_name))

    @defer.inlineCallbacks
    def process(self, baton):
        status_text = util.dict_get_path(baton, self.status_path, Ellipsis)

        if status_text is not Ellipsis:
            # get the api by waiting for the dependency to become ready
            api = yield self.api_dependency.wait_for_resource()

            try:
                status = yield api.update_status(status_text)
            except error.TweepError as e:
                log.error('Error updating the status: %s' % e)
            else:
                log.warn('Updated the status of %r to %r' % (status.user.screen_name, status.text))

        else:
            log.warn('No value at %r in the baton. Not updating the status.' % self.status_path)

        # return the baton unchanged
        defer.returnValue(baton)
