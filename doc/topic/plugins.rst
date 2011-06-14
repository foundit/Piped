Piped plugins
=============


Plugins are loaded by piped can be used in the pipeline configurations by creating a
:ref:`plugin bundle <topic-plugin-bundles>`.

.. _topic-plugin-processor:

Processor plugins
-----------------

A *processor* is a class that implements the :class:`~piped.processing.IProcessor` interface.

Processors are referenced in configuration files by their name. A processor name should be a short
hyphenated string that clearly states what the processor is doing. Names should read
as *actions* and if possible, be descriptive about how it works.

===================== ======================
 Bad processor names   Good processor names
===================== ======================
 attribute-renamer     rename-attribute
 compile-pattern       compile-regex-pattern
===================== ======================




Lifetime of a processor
^^^^^^^^^^^^^^^^^^^^^^^

When pipelines are created, the processor name is used to look up the processor class. The processor
is then instantiated with the configuration keys and values as keyword arguments to their
constructor.

After being initialized, the processor will receive a call to its :meth:`~piped.processing.IProcessor.configure`
method. This gives the processor access to the current :doc:`runtime environment </topic/runtime_environment>`.

The processor can use the runtime environment to access the configuration or request dependencies from
resource providers, among other things.

Having been configured, the processor is eligible to be asked to process batons when the pipeline reaches
the processor instance in its processor graph. This is done via a call to :meth:`~piped.processing.IProcessor.process`.
This is the only method that is **required** by every processor to implement if it inherits from
:class:`piped.processors.base.Processor`.

In order to determine which consumers of the current processor should be used, the pipeline calls
:meth:`~piped.processing.IProcessor.get_consumers` after it has processed the baton. This allows every
processor to override and explicitly return a list of processors that should receive its processed baton. This
is used in processors such as :ref:`lambda-decider` and :ref:`stop` in order to affect the rest of the processing
graph evaluation.


Example processor
^^^^^^^^^^^^^^^^^

Below is a simple processor that shows the steps required to implement a processor plugin.

.. code-block:: python

    from zope import interface

    from piped import util, processing
    from piped.processors import base


    class MyTestProcessor(base.Processor):
        # the following line states that this class is an IProcessor, and makes instances of
        # it available for use in the pipelines:
        interface.classProvides(processing.IProcessor)

        # every processor needs a name, which is used in the configuration:
        name = 'my-test-processor'

        def process(self, baton):
            # perform any operation you want with the baton here.
            return baton


Some more specific :ref:`base classes <reference-processor-base-classes>` may be used to avoid having
to perform some basic tasks, such as getting/setting input from and to the baton.


For further details about what attributes and methods a processor may use, see
:class:`piped.processing.IProcessor`.


.. _topic-plugin-provider:

Provider plugins
----------------

A *provider* is a class that implements the :class:`~piped.resource.IResourceProvider` interface.


Lifetime of a provider
^^^^^^^^^^^^^^^^^^^^^^

All providers are instantiated without any arguments when the plugin system is loaded. After being instantiated,
their :meth:`~piped.resource.IResourceProvider.configure` method is called. This gives the provider access
to the current :doc:`runtime environment </topic/runtime_environment>`.

Most providers use the configuration manager in the runtime environment to prepare for providing one or
more resources. The providers then use the :doc:`resource manager </topic/resources>` to register themselves
as providers for named resources.

After a provider has registered itself as a provider for a resource, it may
receive requests for that resource from other parts of the system. These requests arrive via
:meth:`~piped.resource.IResourceProvider.add_consumer`.

The first argument to :meth:`~piped.resource.IResourceProvider.add_consumer` is a
:class:`piped.dependencies.ResourceDependency` instance, and the provider must ensure that
:meth:`piped.dependencies.ResourceDependency.on_resource_ready` is called with the resource instance when
the resource becomes available and that :meth:`piped.dependencies.ResourceDependency.on_resource_lost` if the
resource becomes unavailable. This allows piped to propagate the availability of resources to the rest of
the system.

Whether the resource is created during the call to :meth:`~piped.resource.IResourceProvider.configure` or
during the call to :meth:`~piped.resource.IResourceProvider.add_consumer` is entirely up to the provider, but
lazily creating resources is generally recommended in order to avoid creating resources that are not actually
used by the process.

.. note:: The providers are instantiated and configured in arbitrary order.


Example provider
^^^^^^^^^^^^^^^^

The following provider shows the steps required in order to create a provider plugin. The example
provider provides random integers or floats in the range 0-100 as named resources:

.. code-block:: python

    import random

    from zope import interface

    from piped import exceptions, log, resource, util


    class MyRandomProvider(object):
        """ Provides random numbers between 0 and 100.

        Example configuration::

            random:
                foo: int
                bar: float

        The above named random numbers are made available as ``random.foo` and
        ``random.bar``.

        """
        interface.classProvides(resource.IResourceProvider)

        def configure(self, runtime_environment):
            self.random_configs = runtime_environment.get_configuration_value('random_configs', dict())

            # tell the resource manager that we can provide the named random numbers:
            resource_manager = runtime_environment.resource_manager
            for random_name, type in self.random_configs.items():
                resource_manager.register('random.%s' % random_name, provider=self)

        def add_consumer(self, resource_dependency):
            # since we registered for 'random.X', we can look up the random_name requested by splitting:
            _, random_name = resource_dependency.provider.split('.')

            random_type = self.random_configs[random_name]

            # provide the resource_dependency with a random number as a resource:
            if random_type == 'int':
                resource_dependency.on_resource_ready(random.randint(0, 100))
            else:
                resource_dependency.on_resource_ready(random.random()*100)


For further details about what attributes and methods a provider may use, see
:class:`piped.resource.IResourceProvider`.



.. _topic-plugin-bundles:

Using custom processors or providers
------------------------------------

Custom processors and providers are made available to Piped by writing plugin bundles. A plugin
bundle is simply either a package or module on ``sys.path`` that contains processor or provider
plugins.

They are made available to the piped process by specifying them under the "plugins.bundles" configuration
path as named mappings to lists of packages.


.. code-block:: yaml

    plugins:
        bundles:
            my_bundle:
                - my_package.test_processors

A common pattern during the development of custom plugins and providers is to set the ``PYTHONPATH``
environment variable prior to starting piped:

.. code-block:: bash

    # globally ...
    $ export PYTHONPATH=.
    $ piped -nc my_config.yaml
    (...)

    # ... or inline ...
    $ PYTHONPATH=. piped -nc my_config.yaml
    (...)