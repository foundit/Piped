Resources
=========

.. currentmodule: piped.resource

What is a resource, and what is it used for?

A resource may be any Python object.


Consuming and requesting resources
----------------------------------

Resources are requested by creating an :class:`~piped.dependencies.ResourceDependency` instance,
which contains the logical name of a provided resource and an optional resource configuration.

The resource dependency must be :ref:`resolved <topic-resources-resolving>` before being used.

After the resource dependency has been resolved, the resource can be accessed by using
:meth:`~piped.dependencies.ResourceDependency.get_resource` if the resource is ready.
Otherwise, :meth:`~piped.dependencies.ResourceDependency.wait_for_resource` can be used, which
waits asynchronously until the resource is ready.


For more information on the lifecycle of dependencies, see :ref:`topic-dependencies-lifecycle`.

Using resource dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here is an example processor that depends on a pipeline as a resource:

.. code-block:: python

    from twisted.internet import defer
    from zope import interface

    from piped.processors import base
    from piped import processing

    class MyProcessor(base.Processor):
        interface.classProvides(processing.IProcessor)
        def configure(self, runtime_environment):
            dm = runtime_environment.dependency_manager

            # request the dependency, returns a ResourceDependency instance
            self.pipeline_resource = dm.add_dependency(self, dict(provider='pipeline.name_of_pipeline'))

        @defer.inlineCallbacks
        def process(self, baton):
            # wait for the pipeline to become available
            pipeline = yield self.pipeline_resource.wait_for_resource()

            # process the baton in the pipeline, and wait until the processing is done
            yield pipeline.process(baton)

            defer.returnValue(baton)


In order to react to the availabilty of resources, use the ``on_ready``
and ``on_lost`` events (see :class:`~piped.dependencies.IDependency`).

.. code-block:: python

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager

        self.resource_dependency = dm.add_dependency(self, dict(provider='empty.dict'))

        def on_ready(dependency):
            print 'Resource %s ready.'%dependency

        def on_lost(dependency):
            print 'Resource %s lost, reason: %s.'%(dependency, reason)

        self.resource_dependency.on_ready += on_ready
        self.resource_dependency.on_lost += on_lost


Creating a resource dependency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Resource dependencies are usually added during the configuration-phase of starting a Piped
process, where the :doc:`runtime environment <runtime_environment>` is available. The runtime
environment contains the DependencyManager, which is used to create resource dependencies.

There are 2 supported ways of creating resource dependencies:

#. Using :meth:`~piped.dependencies.DependencyManager.add_dependency`, which adapts the
   input arguments to :class:`~piped.dependencies.IDependency` and returns the dependency. *(recommended)*
#. Using :meth:`~piped.dependencies.DependencyManager.create_dependency_map`, which
   creates a :class:`~piped.dependencies.DependencyMap`.


Using a dependency map is convenient when requesting many resources.

.. code-block:: python

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager

        # requesting a resource using add_dependency:
        self.resource_dependency = dm.add_dependency(self, dict(provider='empty.dict'))

        # requesting a resource of the same type using a dependency map
        self.dependencies = dm.create_dependency_map(self, resource=dict(provider='empty.dict'))



.. _topic-resources-resolving:

Resolving a resource dependency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similar to :doc:`other dependencies <dependencies>`, :class:`~piped.dependencies.ResourceDependency` instances must
be resolved before they can be used. The :class:`~piped.dependencies.DependencyManager` is
responsible for resolving all dependencies.

.. note:: If the resource dependency is attached to a :class:`~piped.dependencies.DependencyManager` during
    the Piped :ref:`startup process <topic-piped-bootstrapping>`, this is done automatically after all
    the providers and processors have been loaded and configured.


If the resource dependency is attached to the dependency manager after all the providers and
processors have been configured, :meth:`piped.dependencies.DependencyManager.resolve_initial_states`
must be called in order to resolve dependency. See :ref:`topic-dependencies-resolving` for more
details about when dependency resolving.




Creating and providing resources
--------------------------------


In order to be able to make a resource available to other components in Piped, an instance
must implement the :class:`IResourceProvider` interface.



Registering resources
^^^^^^^^^^^^^^^^^^^^^

Resources are registered by calling :meth:`ResourceManager.register`.

.. code-block:: python

    def configure(self, runtime_environment):
        rm = runtime_environment.resource_manager
        rm.register('empty.dict', provider=self)
        rm.register('empty.list', provider=self)



Providing resources
^^^^^^^^^^^^^^^^^^^

After registering with the :class:`ResourceManager`, the provider may receive calls to its
:meth:`IDependency.add_consumer` method.


.. code-block:: python

    def add_consumer(self, resource_dependency):
        resource = dict()

        if resource_dependency.provider == 'empty.list':
            resource = list()

        resource_dependency.on_resource_ready(resource)



Reacting to availability
^^^^^^^^^^^^^^^^^^^^^^^^

on_resource_ready vs on_resource_lost - chaining osv?