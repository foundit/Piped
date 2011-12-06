Testing Piped Services
======================

.. currentmodule:: piped_status_testing


A "status test" is a variant of unit tests that are run inside a live process.

Status tests can be used for system tests, integration tests, health-checks, etc.


A simple status test processor
------------------------------

Inheriting from :class:`~processors.StatusTestProcessor` is the easiest way to create a
status test processor as it takes care of creating a loader, loading the test suites and running them
with a given reporter.

A basic example::

    from zope import interface

    from piped_status_testing import statustest, processors
    from piped import processing

    class TestNothing(processors.StatusTestProcessor):
        interface.classProvides(processing.IProcessor)
        name = 'test-nothing'

        class TestNothing(statustest.StatusTestCase):

            # note that the tests are prefixed with 'statustest_', not 'test_', as regular unit tests.
            def statustest_nothing(self):
                pass


For a more in-depth example of how to create a status test processor and how to depend on resources, see
the :class:`processors.StatusTestProcessor` reference documentation.

These test processors should be put in a module and loaded as a :ref:`plugin bundle <topic-plugin-bundles>`.


One shot testing
----------------

The following configuration runs a list of test processors inside the same process as the included service:

.. code-block:: yaml

    includes:
        - path/to/your/service_config.yaml

    plugins:
        bundles:
            my_bundle:
                - my_package

    system-events:
        startup:
            test: pipeline.system-testing.start

    pipelines:
        system-testing:    
            start:
                chained_consumers:
                    - create-statustest-reporter
                    - run-pipeline:
                        pipeline: .run
                    - wait-for-statustest-reporter:
                        done: True
                    - shutdown

                chained_error_consumers:
                    - print-failure-traceback
                    - shutdown

            # add any test-processors to this test-pipeline
            run:
                - test-nothing


After the tests have been run, a summery of the test results are printed to the log and the process is stopped.


Continuous testing
------------------

Status tests can also be run continuous:

.. code-block:: yaml

    includes:
        - path/to/your/service_config.yaml

    plugins:
        bundles:
            my_bundle:
                - my_package

    ticks:
        interval:
            system_testing.start:
                # run the tests every 5 seconds:
                interval: 5
                processor: pipeline.system-testing.start

    pipelines:
        system-testing:
            start:
                chained_consumers:
                    - create-statustest-reporter
                    - run-pipeline:
                        pipeline: .run
                    - wait-for-statustest-reporter:
                        done: True

            # add any test-processors to this test-pipeline
            run:
                - test-nothing


In this configuraiton, the tests are run every 5 seconds as long as the process is running.


Health checks via HTTP
----------------------

The above tests will output their results to the logging mechanism, but creating a
:mod:`http view<piped.providers.web_provider>` is also possible by adding this to the
`continuous testing`_ configuration:

.. code-block:: yaml

    web:
        system-testing:
            port: 8080
            routing:
                __config__:
                    processor: pipeline.web.system-testing

    pipelines:
        web:
            system-testing:
                - exec-code:
                    input_path: request
                    code: |
                        # we patch the request object since it does not have a flush() method
                        input.flush = lambda: None
                        # wrap the test reporting results in a <pre> tag for simple formatting
                        input.write('<pre>')
                        return input

                - create-statustest-reporter:
                    stream: request

                # run the same pipeline as the continuous testing:
                - run-pipeline:
                    pipeline: system-testing.run

                - wait-for-statustest-reporter:
                    done: true

                - exec-code:
                    input_path: request
                    output_path: body
                    code: |
                        # close the <pre> tag
                        input.write('</pre>')
                        return ''

                - write-web-response

