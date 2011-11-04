import json

from zope import interface
from twisted.internet import defer

from piped.contrib.status_testing import statustest, processors
from piped import processing


class TestClientProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-client'

    class TestClient(statustest.StatusTestCase):

        def setUp(self, sum_pipeline, square_pipeline):
            self.sum_pipeline = sum_pipeline
            self.square_pipeline = square_pipeline

        @defer.inlineCallbacks
        def statustest_simple_sum(self):
            input = json.dumps(dict(numbers=[1,2,3]))
            results = yield self.sum_pipeline(input)
            output = json.loads(results[0])
            self.assertEquals(output['sum'], 14)

        @defer.inlineCallbacks
        def statustest_simple_square(self):
            input = json.dumps(dict(numbers=[1,2,3]))
            results = yield self.square_pipeline(input)
            output = json.loads(results[0])
            self.assertEquals(output['square'], 36)

    def configure(self, runtime_environment):
        self.dependencies = runtime_environment.create_dependency_map(self,
            sum_pipeline = dict(provider='pipeline.zmq.sum'),
            square_pipeline = dict(provider='pipeline.zmq.square')
        )

    def get_namespace(self, baton):
        return self.dependencies