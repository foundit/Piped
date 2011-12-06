from zope import interface
from twisted.internet import defer
from twisted.web import client, error

from piped.plugins.status_testing import statustest, processors
from piped import processing


class TestTutorialProcessor(processors.StatusTestProcessor):
    interface.classProvides(processing.IProcessor)
    name = 'test-tutorial'

    def configure(self, runtime_environment):
        # request a reference to the perspective broker client that is used by the web server
        # and a reference to the pb server, which the pb client connects to.
        self.dependencies = runtime_environment.create_dependency_map(self,
            pb_client = dict(provider='pb.client.tutorial'),
            pb_server = dict(provider='pb.server.tutorial')
        )

    # this returned namespace is given to the setUp function of the tests
    def get_namespace(self, baton):
        return self.dependencies

    
    class TestTutorial(statustest.StatusTestCase):

        def setUp(self, pb_client, pb_server):
            self.pb_client = pb_client
            self.pb_server = pb_server

        # note that the tests are prefixed with 'statustest_', not 'test_', as regular unit tests.
        @defer.inlineCallbacks
        def statustest_pb_server(self):
            """ The perspective broker server should return a greeting. """
            root = yield self.pb_client.getRootObject()
            result = yield root.callRemote('test')
            self.assertEquals(result, 'Hello from the PB server.')

        @defer.inlineCallbacks
        def statustest_web_server(self):
            """ The web server shoulr return the greeting from the pb server. """
            result = yield client.getPage('http://localhost:8080')
            self.assertEquals(result, 'HELLO FROM THE PB SERVER.')

        @defer.inlineCallbacks
        def statustest_web_server_without_pb(self):
            """ If the pb server is stopped, the web server should return an error status. """

            # stop the perspective broker server for the duration of this test:
            self.pb_server.stopService()
            self.addCleanup(self.pb_server.startService)

            try:
                yield client.getPage('http://localhost:8080')
                self.fail('Expected an error to be raised.')
            except error.Error as e:
                self.assertEquals(e.status, '500')

                # since we've turned on debugging for localhost, we should get a traceback:
                self.assertIn('web.Server Traceback', e.response)