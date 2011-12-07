from StringIO import StringIO

from zope import interface
from twisted.internet import defer
from twisted.web import client

from piped.plugins.status_testing import statustest, processors
from piped import processing, util


class TestTwitterProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-twitter'

    class TestTwitter(statustest.StatusTestCase):
        timeout = 10

        def setUp(self, tracked_pipeline):
            self.tracked_pipeline = tracked_pipeline

        @defer.inlineCallbacks
        def statustest_status_generated(self):
            test_text = 'this is a tweet'
            fake_status_baton = dict(status=dict(text=test_text), screen_name='test', followers_count='0')
            yield self.tracked_pipeline(fake_status_baton)

            page = yield client.getPage('http://localhost:8080/generate')

            self.assertIn(test_text, page)

    
    def configure(self, runtime_environment):
        self.dependencies = runtime_environment.create_dependency_map(self,
            tracked_pipeline = dict(provider='pipeline.status.tracked')
        )

    def get_namespace(self, baton):
        return self.dependencies