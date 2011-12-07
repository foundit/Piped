Processing
==========


.. module:: piped.processing


The ``piped.processing`` module contains core classes that are used in the
pipeline processing.


Runtime environment
-------------------

.. autoclass:: RuntimeEnvironment

An instance of this class is sent to many of the components in Piped, via a call to the
components configure method::

    def configure(self, runtime_environment):
        pass


Getting configuration values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. method: RuntimeEnvironment.get_configuration_value

Values can be retrieved from the configuration by using ``get_configuration_value``, which
is an alias for :meth:`.conf.ConfigurationManager.get`::

    my_pipeline_config = runtime_environment.get_configuration_value('pipelines.my_pipeline')



Requiring dependencies
^^^^^^^^^^^^^^^^^^^^^^

.. method: RuntimeEnvironment.create_dependency_map

The runtime environment can be used to create :class:`.dependencies.DependencyMap`\s by calling
``create_dependency_map`` which is an alias for :meth:`.dependencies.DependencyManager.create_dependency_map`::

    my_pipeline_config = runtime_environment.get_configuration_value('pipelines.my_pipeline')




Processors
----------

All processors must implement the ``IProcessor`` interface:

.. autointerface:: IProcessor
    :member-order: bysource
    :members:



.. _reference-processor-base-classes:

Processor base classes
^^^^^^^^^^^^^^^^^^^^^^


.. automodule:: piped.processors.base
    :members: Processor, InputOutputProcessor, MappingProcessor



Status test processors
^^^^^^^^^^^^^^^^^^^^^^^


.. autoclass:: piped_status_testing.processors.StatusTestProcessor
    :members:


.. :

    Processing graph
    ----------------

    .. autoclass:: ProcessorGraph
        :members:


    .. autoclass:: ProcessorGraphBuilder
        :members:

    Evaluators
    ----------


    .. autoclass:: TwistedProcessorGraphEvaluator
        :members: