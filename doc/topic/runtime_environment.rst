The Runtime Environment
=======================


The :class:`piped.processing.RuntimeEnvironment` is an object that provides access to shared resources.


All pipelines, processors and providers are configured with the runtime environment before being used.


Through the runtime environment, resources in Piped gain access to the application, configuration, dependencies and resources.


.. _topic-runtime_environment-contains:

What does it contain
--------------------

 * Application object
 * Configuration manager
 * Dependency manager
 * Resource manager


Configuration
--------------

The runtime environment is used to configure most components in Piped, usually via a call to
``component.configure(runtime_environment)``.

This gives the component access to the the :ref:`shared resources <topic-runtime_environment-contains>` of the
runtime environment, and is used for everything from accessing the :doc:`configuration <configuration>`,
:doc:`registering and requesting resources <resources>` and working with :doc:`dependencies <dependencies>`.