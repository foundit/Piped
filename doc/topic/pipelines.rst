Working with pipelines
======================

Pipelines are directed graphs where the nodes are processors and the edges are their consumers.

Every pipeline may have multiple inputs (sources) and outputs (sinks), which are processors.

When the pipeline processes a :doc:`baton <batons>`, the baton is sent to every source in the processor graph.


Pipelines are provided by :class:`~piped.providers.pipeline_provider.PipelineProvider`.

.. highlight:: yaml


Creating a simple pipeline
--------------------------

Pipelines are created under the ``pipelines`` configuration key::

    pipelines:
        my_pipeline:
            # pipeline definition


Pipeline definitions
^^^^^^^^^^^^^^^^^^^^

Pipelines can be defined in multiple ways:



.. _topic-pipelines-list:

Using a :class:`list`
"""""""""""""""""""""

A list of processors has only one source::

    pipelines:
        my_pipeline:
            - # processor definition
            - # processor definition

Each processor in the list will get the baton from the previous processor.
This is the same as using :ref:`chained_consumers <topic-pipelines-chained_consumers>`, except
it saves typing.



Using ``consumers``
"""""""""""""""""""

A dictionary containing the key ``consumers`` has multiple sources::

    pipelines:
        my_pipeline:
            consumers:
                - # processor definition
                - # processor definition


.. _topic-pipelines-chained_consumers:

Using ``chained_consumers``
"""""""""""""""""""""""""""

A dictionary containing the key ``chained_consumers``::

    pipelines:
        my_pipeline:
            chained_consumers:
                - # processor definition
                - # processor definition

Each processor in the list will get the baton from the previous processor.


.. _topic-pipelines-chained_consumers-rewriting:

When using the list of processors or ``chained_consumers``, the pipeline definition is rewritten
before the processors are instantiated. Every processor in a ``chained_consumers`` list is added to its
preceeding processors list of consumers.

This means the following two pipeline definitions are equivalent::

    my_pipeline:
        - a:
            consumers:
                - b:
                    consumers:
                        - c
                        - d
        - e
        - f

    my_pipeline:
        consumers:
            - a:
                consumers:
                    - b:
                        consumers:
                            - c
                            - d
                    - e:
                        consumers:
                            - f

Which will be turned into the this processing graph:

.. digraph:: chained_consumers

    a -> b
    b -> c
    b -> d
    a -> e
    e -> f


If both ``chained_consumers`` and ``consumers`` are defined, either explicitly (both keys being used in the configuration)
or implicitly (processors inside a list of ``chained_consumers`` that define their own ``chained_consumers``). For example,
consider the following pipeline definition:

.. code-block:: yaml

    my_pipeline:
        - a
            chained_consumers:
                - b
                - c
        - d
        - e

Since the pipeline uses the shorthand syntax for ``chained_consumers`` (see :ref:`topic-pipelines-list`), all processors
in the list are added to the preceding processors list of consumers. Applying this once transforms the pipeline definition to this:

.. code-block:: yaml

    my_pipeline:
        consumers:
            - a:
                chained_consumers:
                    - b
                    - c
                consumers:
                    - d:
                        consumers:
                            - e

This process is done recursively for all the processors, and ``chained_consumers`` takes precedence over ``consumers`` when it
comes to the ordering of the consumers, which results in the following final pipeline, noting that ``b`` is the first consumer
of ``a``:

.. code-block:: yaml

    my_pipeline:
        consumers:
            - a:
                consumers:
                    - b:
                        consumers:
                            - c
                    - d:
                        consumers:
                            - e


Inlining a pipeline
"""""""""""""""""""

A pipeline may be used instead of a processor by using a dict with a single key ``inline-pipeline``
instead of a processor definition. The configuration for that pipeline is then used inline at
that processors place in the processing graph.

For example::

    my_pipeline:
        - a
        - b
        - inline-pipeline: another_pipeline
        - c

    another_pipeline:
        - d
        - e

Is equivalent to::

    my_pipeline:
        - a
        - b
        - d
        - e
        - c

.. note:: While this works well for small pipelines, it is generally recommended that the
    ``run-pipeline`` processor is used, as it has more configuration options and multiple
    instances of ``run-pipeline`` is able to re-use the same pipelines instead of creating
    new ones.


If the inline pipeline has consumers in the outer pipeline (in the example above, the
inline pipeline has the consumer ``c`` in the outer pipeline), that consumer is added
to every sink in the inline pipeline. For example::

    my_pipeline:
        - a
        - inline-pipeline: another_pipeline
        - b

    another_pipeline:
        - c:
            consumers:
                - d
        - e

Results in the following processor graph:

.. digraph:: inline_pipeline_consumers

    a -> c

    c -> d
    c -> e

    e -> b
    d -> b

The result is that ``b`` will receive two batons for each baton ``a`` receives.

In order to ensure that a pipeline only has one sink, an explicit sink processor may be used::

    another_pipeline:
        - c:
            consumers:
                - d:
                    consumers:
                        - existing: sink
        - e
        - f:
            id: sink

This pipeline results in the following processor graph:

.. digraph:: inline_pipeline_consumers_sink

    a -> c

    c -> d
    d -> f
    c -> e
    e -> f

    f -> b

The ``passthrough`` processor is a good candidate to use as a sink.


Nested pipelines
^^^^^^^^^^^^^^^^

Pipeline definitions may be nested within the pipelines configuration::

    pipelines:
        my_namespace:
            pipeline_1:
                # pipeline definition 1
            pipeline_2:
                # pipeline definition 2

This may be used to group related pipelines together in a logical structure.

.. note:: It is not possible to nest pipeline definitions within each other.

Processor definitions
^^^^^^^^^^^^^^^^^^^^^

In order to look up and instantiate a processor, a processor definition includes:

#. A processor name.

#. Its initialization arguments (optional). Not all processors require any arguments, or the
   default arguments may be suitable.


Processors can be defined in multiple ways:


Using the processor name
""""""""""""""""""""""""

If the processor does not require any  options::

    my_pipeline:
        - processor_name



Using a :class:`dict` with a single key
"""""""""""""""""""""""""""""""""""""""

A :class:`dict` with a single key, the processor name, may be used to pass options to the processor::

    my_pipeline:
        - processor_name:
            foo: bar


Using a dict containing the key ``processor``
"""""""""""""""""""""""""""""""""""""""""""""

A dictionary with the ``processor`` key set to the processor name::

    my_pipeline:
        - processor: processor_name
          foo: bar



Special keys used in processor definitions
""""""""""""""""""""""""""""""""""""""""""

id:
    Used to give processors an unique id within a processor graph.

existing:
    Used to refer to other processors by id within a processor graph.

consumers:
    A list of processors. Each processor in the list receives the output
    baton from the current processor.

error_consumers:
    Same as consumers, but used when the processor or one of its consumers
    raises an exception.




.. note:: Depending on how the processor is used within a processor graph, the
    consumers and error_consumers lists may have more elements than its own local
    list of consumers declare, like in the processor ``a`` in the
    :ref:`chained_consumers <topic-pipelines-chained_consumers-rewriting>` example.


Creating processor graphs
-------------------------



A processing tree
^^^^^^^^^^^^^^^^^

Branching::

    my_pipeline:
        - a:
            consumers:
                - b
                - c
                - d

.. digraph:: simple_tree

    a -> b
    a -> c
    a -> d


Merging::

    my_pipeline:
        - a:
            consumers:
                - b:
                    consumers:
                        - existing: e_id
                - c:
                    consumers:
                        - existing: e_id
                - d:
                    consumers:
                        - existing: e_id
        - e:
            id: e_id

.. digraph:: merging

    a -> b
    a -> c
    a -> d
    b -> e
    c -> e
    d -> e


Note that ``e`` will receive three batons for every baton ``a`` receives.


Loops
"""""

A simple loop, similar to "do-while"::

    my_pipeline:
        - a:
            id: a_id
        - b
        - c:
            consumers:
                - existing: a_id
        - d

.. digraph:: simple_loop

    a -> b -> c -> d
    c -> a

.. note:: In order to avoid ``d`` receiving multiple batons for every baton ``a`` receives, ``c`` must
    implement the necessary logic to select which of its consumers it should send its output baton to.

A simple loop, similar to "while-do"::

    my_pipeline:
        - a:
            id: id_a
            consumers:
                - inline-pipeline: my_pipeline.inner
                  consumers:
                    - existing: id_a
        - d

    my_pipeline.inner:
        - b
        - c

.. digraph:: loop_using_another_pipeline

    subgraph outer {
        a -> d
    }

    subgraph chained {
        b -> c
    }

    /* connections between the subgraphs */
    a -> b
    c -> a


.. note:: In order to avoid ``d`` receiving multiple batons for every baton ``a`` receives, ``a`` must
    implement the necessary logic to select which of its consumers it should send its output baton to.


.. Concrete do-while loop::

    my_pipeline:
        - pretty-print:
            id: loop-start
        - increment:
            input_path: n
        - lambda-decider:
            input_path: n
            lambda: 'n: [0] if n <= 10 else [-1]'
            consumers:
                - existing: loop-start
        - pretty-print:
            prefix: 'finished '

..
    .. digraph:: simple_loop_with_exit_condition

        "do-something" -> "increment-counter"
        "increment-counter" -> "lambda-decider"
        "lambda-decider" -> "do-something"
        "lambda-decider" -> "pretty-print"