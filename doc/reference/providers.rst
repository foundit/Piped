Available providers
===================


Providers are classes that add functionality to a Piped process.


.. _provider-contexts:

contexts
--------

.. module:: piped.providers.context_provider

.. autoclass:: ContextProvider
    :members:


.. autoclass:: PersistedContextProvider
    :members:


Shared state
^^^^^^^^^^^^

Contexts can be used to implement shared state. In the above examples, ``my_context`` is a
dictionary, and any changes to that dictionary will be immediately visible to all other
users of the resource.


perspective broker
------------------

.. module:: piped.providers.spread_provider

.. autoclass:: PBServerProvider
    :members:

.. autoclass:: PBClientProvider
    :members:

pipelines
---------

.. module:: piped.providers.pipeline_provider

.. autoclass:: PipelineProvider

For the topic page about how to create pipelines, see :doc:`/topic/pipelines`.

Using a pipeline from a processor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following is an processor stub that processes a baton in a pipeline for every baton it processes::

    def configure(self, runtime_environment):
        # add a dependency to the provided pipeline resource
        dm = runtime_environment.dependency_manager
        self.pipeline_dependency = dm.add_dependency(self, dict(provider='pipeline.my_pipeline'))

    @defer.inlineCallbacks
    def process(self, baton):
        # get the pipeline from the pipeline_dependency
        pipeline = pipeline_dependency.get_resource()

        # create the baton we're going to process:
        baton_to_process = dict(foo='bar')

        # wait until the pipeline has finished processing the baton_to_process:
        yield pipeline.process(baton_to_process)

        # return the baton unchanged
        defer.returnValue(baton)


For the topic page about dependencies, see :doc:`/topic/dependencies`.


smtp
--------

.. module:: piped.providers.smtp_provider

.. autoclass:: SMTPProvider
    :members:


system-events
-------------

.. module:: piped.providers.system_events_provider

.. autoclass:: SystemEventsProvider
    :members:

ticks
-----

.. automodule:: piped.providers.tick_provider
    :members:

web
---

.. currentmodule:: piped.providers.web_provider

The ``web`` provider enables running multiple production quality web servers inside the :program:`piped`
process that use pipelines in order to respond to incoming requests.


Configuration
^^^^^^^^^^^^^

.. automodule:: piped.providers.web_provider
    :members: WebResourceProvider, WebSite

.. autoclass:: WebResource



Responding to requests
^^^^^^^^^^^^^^^^^^^^^^

The baton that is processed in the pipeline, contains the request object, which is an
`twisted.web.server.Request <http://twistedmatrix.com/documents/current/api/twisted.web.server.Request.html>`_
instance.


Example pipeline that returns the clients host name::

    pipelines:
        web:
            show-host:
                - eval-lambda:
                    input_path: request
                    output_path: host
                    lambda: "request: request.getHost()"
                - write-web-response:
                    body_path: host

The first processor fetches the host name from the request, and the second processor writes the response to
the client.



Debugging
^^^^^^^^^

When the pipeline processing results in an :class:`Exception`, the :class:`WebResource` takes care of setting
a proper response code and close the request.

If debugging is turned off or if the client is not allowed to debug, a short html page saying ``Processing Failed`` is
returned.

If debugging is turned on and the client is allowed to debug, the :class:`Exception` traceback is registered with
a :class:`WebDebugger`, which enables the client to evaluate python expressions at every frame in the traceback.

.. warning:: Enabling debugging allows clients to execute arbitrary code within the Piped process.

Debugging can be turned on by using the ``debug`` key on the web site::

    web:
        my_site:
            ...
            debug:
                allow: # list of hostnames/ip addresses
                    - localhost
            ...

.. warning:: Keep in mind that this uses the hostname / ip address as seen by the web server inside the Piped process,
    which may not be correct if for example an proxy is used to access the web server.



Better tracebacks
"""""""""""""""""

More detailed tracebacks with live objects are available if :option:`piped -D` is used to start the process.




Contrib providers
-----------------

database
^^^^^^^^

.. automodule:: piped.contrib.database.providers
    :members:


manholes
^^^^^^^^

.. automodule:: piped.contrib.manhole.providers
    :members: ManholeProvider


zmq
^^^

.. automodule:: piped.contrib.zmq.providers
    :members: ZMQSocketProvider, ZMQPipelineFeederProvider