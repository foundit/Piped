Glossary
========

.. glossary::

    baton
        An object passed to processors in a pipeline. See :doc:`topic/batons`.

    key_path
        A string that contains dot-separated path segments that are used to look up values
        from nested dictionaries.

        .. seealso::
            :func:`piped.util.dict_get_path` and :func:`piped.util.dict_set_path`

    pipeline
        A :term:`processor graph` that can process :term:`batons <baton>`.

    processor
        A processor is an object that can process a baton.

    processor graph
        A graph of processors. Processors are connected to each other in a graph structure
        in order to form a pipeline.


    provider
        A provider is an object that add some functionality to the system.

        .. seealso::
            Provider reference documentation is located under :doc:`reference/providers`.
