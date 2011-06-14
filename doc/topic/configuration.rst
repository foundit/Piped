Configuration
=============

.. automodule:: piped.conf

.. currentmodule:: piped.conf

The configuration is loaded by a :class:`ConfigurationManager` that takes care of
loading and :ref:`merging included configuration files<topic-configuration-includes>`.

Configuration is loaded as part of the :program:`piped` :doc:`startup process</topic/startup>`.


What is a configuration file?
-----------------------------

A configuration file is a file that contains a `YAML <http://pyyaml.org/>`_ mapping. A
`YAML mapping <http://pyyaml.org/wiki/PyYAMLDocumentation#Blockmappings>`_ is essentially
a Python :class:`dict` that may contain any other Python primitive or object.

.. highlight:: yaml

Example valid configuration file:

.. code-block:: yaml

    pipelines:
        ...

Invalid configuration files::

    [1,2,3]

    - 1
    - 2
    - 3

    strings/numbers

An empty configuration file is allowed and is treated as an empty :class:`dict`.


.. note:: Even though loading a YAML file can construct any object by using different
    constructors, providers usually only use primitive types, such as :class:`dict`,
    :class:`list`, :class:`tuple`, :class:`str`, :class:`int`, :class:`float` and so on.

Additional YAML constructors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In YAML, constructors are responsible for creating Python objects for the nodes
in the configuration file. Constructors are referenced by tagging the YAML node
with ``!constructor_name``.

``!pipedpath`` is a constructor that is available in all configuration files:

**!pipedpath**
    constructs :class:`twisted.python.filepath.FilePath` instances relative to the
    Piped source root path. This is used to reference bundled data.

Example that uses !pipedpath to :ref:`include <topic-configuration-includes>` the default built-in configuration file::

    includes:
        - !pipedpath conf.yml



The contents of a configuration file
------------------------------------

The configuration is used by :doc:`providers </reference/providers>` and
:doc:`processors </reference/processors>`, which are loaded as plugins when Piped
is :doc:`started <startup>`.


See the :doc:`overview of available providers</reference/providers>` for an overview
over root configuration keys that have special meaning.


.. _topic-configuration-includes:

Including other configuration files
-----------------------------------

Includes are specified under the top level key ``includes`` in the configuration files.

.. code-block:: yaml

    includes:
        - path_string
        - /absolute_path_string
        - ~/using/a/home/folder
        - $VAR/using/an/environment/variable


Included configuration files may specify their own includes, which are loaded recursively.


Merging
^^^^^^^

When a configuration file is loaded, all included configuration files are loaded and merged into the
resulting configuration with the following precedence rules:

- The current configuration takes precedence before any included configuration.
- Included files are loaded in the list-order, and the latest configuration files
  takes precedence over the previously included files.

Examples
""""""""

Given the following configuration files:

foo.yaml::

    includes:
        - bar.yaml

    data:
        foo: 42
        key: foo_value

bar.yaml::

    data:
        bar: 93
        key: bar_value

Loading ``foo.yaml`` results in the following configuration::

    data:
        bar: 93
        foo: 42
        key: foo_value

Loops
^^^^^

If there are loops in the include directives, the last include directive encountered
that causes the loop prints a warning and is skipped.

Consider these two configuration files:

foo.yaml::

    includes:
        - bar.yaml

    foo: foo
    number: 42

bar.yaml::

    includes:
        - foo.yaml

    bar: bar
    number: 93

Loading ``foo.yaml`` results in the following configuration::

    bar: bar
    foo: foo
    number: 42

While loading ``bar.yaml`` results in the following configuration::

    foo: foo
    bar: bar
    number: 93



Runtime changes
---------------

Some providers and processors provide runtime access to the configuration. While the
configuration is not marked read-only, runtime changes are not supported, and may
cause undesirable or undetermined behaviour, depending on how the provider or processor
accesses the configuration values, as the provider may have copied the configuration file
in order to perform some inline changes to it.

Instead, if you need to change the configuration of a running Piped instance, consider
doing it via the specific provider or processor you want to configure.
