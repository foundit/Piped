import time
import json

from zope import interface
from twisted.web import client
from twisted.internet import defer

from piped.plugins.status_testing import statustest, processors
from piped import processing


client.HTTPClientFactory.noisy = False


class TestRPCProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-rpc'

    class TestRPC(statustest.StatusTestCase):
        
        @defer.inlineCallbacks
        def get_result(self, *numbers):
            numbers = [str(num) for num in numbers]
            result_json = yield client.getPage('http://localhost:8080/?n=%s'%'&n='.join(numbers))
            defer.returnValue(json.loads(result_json))
            
        @defer.inlineCallbacks
        def statustest_single_number(self):
            result = yield self.get_result(42)
        
            self.assertEquals(result, dict(sum=42**2, square=42**2, diff=0))

        @defer.inlineCallbacks
        def statustest_ten_numbers(self):
            result = yield self.get_result(1,2,3,4,5,6,7,8,9,10)
            
            self.assertEquals(result, dict(sum=385, square=3025, diff=2640))
            
            
class TestRPCWithoutWorkersProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-rpc-without-workers'

    class TestRPCWithoutWorkers(statustest.StatusTestCase):
        
        def setUp(self, poller):
            self.poller = poller
            # we temporarily stop the zmq poller to prevent the workers from receiving any messages
            self.poller.stopService()
            
        def tearDown(self):
            self.poller.startService()

        @defer.inlineCallbacks
        def statustest_single_number(self):
            started = time.time()
            result = yield client.getPage('http://localhost:8080/?n=42')
            finished = time.time()
            
            # the server should have given the workers two seconds to produce both partial responses:
            self.assertTrue((finished-started) > 2)
            # and our reap-baton pipeline should have provided us with a fallback response.
            self.assertEquals(json.loads(result), dict(success=False))
            
            
    def configure(self, runtime_environment):
        self.dependencies = runtime_environment.create_dependency_map(self,
            poller = dict(provider='zmq.poller')
        )
        
    def get_namespace(self, baton):
        return dict(poller=self.dependencies.poller)