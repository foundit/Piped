import random

from tweepy import error
from zope import interface
from twisted.internet import defer

from piped import util, processing
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
                print 'Error updating the status: %s' % e
            else:
                print 'Updated the status of %r to %r' % (status.user.screen_name, status.text)

        else:
            print 'No value at %r in the baton. Not updating the status.' % self.status_path

        # return the baton unchanged
        defer.returnValue(baton)


class MarkovTrainer(base.Processor):
    # Make instances of this processor available for use in the pipelines:
    interface.classProvides(processing.IProcessor)

    # every processor needs a name, which is used in the configuration:
    name = 'train-markov'

    def __init__(self, input_path, context_name='markov', **kw):
        super(MarkovTrainer, self).__init__(**kw)

        self.input_path = input_path
        self.context_name = context_name

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.context_dependency = dm.add_dependency(self, dict(provider='context.%s'%self.context_name))

    def process(self, baton):
        markov = self.context_dependency.get_resource()
        input = util.dict_get_path(baton, self.input_path)

        # word_1 and _2 is two previous words. We use None as sentence start/stop markers.
        word_1, word_2 = None, None

        for word in input.lower().split():
            if word.startswith('@') or word.startswith('#') or word == 'rt':
                # we ignore usernames, hashtags and the pretty meaningless word "RT"
                continue

            markov.setdefault((word_1, word_2), list()).append(word)

            word_1, word_2 = word_2, word

        # Make sure that the last word can end a sentence:
        if word_2 is not None:
            markov.setdefault((word_1, word_2), list()).append(None)

        # return the baton unchanged
        return baton


class MarkovGenerator(base.Processor):
    # Make instances of this processor available for use in the pipelines:
    interface.classProvides(processing.IProcessor)

    # every processor needs a name, which is used in the configuration:
    name = 'generate-markov'

    def __init__(self, output_path='generated', context_name='markov', **kw):
        super(MarkovGenerator, self).__init__(**kw)

        self.output_path = output_path
        self.context_name = context_name

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.context_dependency = dm.add_dependency(self, dict(provider='context.%s'%self.context_name))

    def process(self, baton):
        markov = self.context_dependency.get_resource()

        word_1, word_2 = None, None

        sentence = list()
        length = 0

        # we must at least have room for a space in order to
        while length < 140:
            # pick a random word from our model:
            word = random.choice(markov[(word_1, word_2)])

            sentence.append(word)
            length += len(word or '')

            # we used None to signify the end of a sentence:
            if word is None or length > 140:
                sentence.pop() # remove the None from the sentence
                break

            word_1, word_2 = word_2, word

        util.dict_set_path(baton, self.output_path, ' '.join(sentence))

        return baton