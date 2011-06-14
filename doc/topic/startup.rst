Starting piped
==============

Piped is usually run as a stand-alone process by using the :program:`piped` command line tool
with a configuration.


.. _topic-piped-daemon:
.. program:: piped

The Piped Process Daemon
------------------------

``piped`` is a process server.

The process is bootstrapped in the following order:

.. _topic-piped-bootstrapping:

#. A :doc:`runtime environment <runtime_environment>` is created.

#. The :doc:`configuration <configuration>` is loaded. This includes handling configuration
   :ref:`includes <topic-configuration-includes>` and :ref:`overrides <piped-configuration-overrides>`.

#. :doc:`Resource providers <resources>` are instantiated and configured. This step cascades into creating
   any required pipelines.

#. All :doc:`dependencies <dependencies>` are resolved.

If any of the above steps results in a failure, the process stops.


Options
^^^^^^^

piped supports the following options, available by running ``piped --help``:


.. cmdoption:: -n, --nodaemon

    Don't daemonize and dont use the default umask of 0077.

.. cmdoption:: -l <logfile>, --logfile <logfile>

    log to a specified file, - for stdout. %%d in the filename will be replaced with "[config_basename].log" (default: "-")

.. cmdoption:: -p <pidfile>, --pidfile <pidfile>

    Name of the pidfile. %%d in the filename will be replaced by the default. (default: "[config_basename].pid")

.. cmdoption:: -d <dirname>, --rundir <dirname>

    Change to the supplied directory before running. (default: .)

.. cmdoption:: -c <configfile>, --conf <configfile>

    Configuration file to use.

.. cmdoption:: -D

    Prevent cleaning of failures and gives more detailed tracebacks at the
    cost of using more memory and cpu. Useful for debugging.

.. cmdoption:: -O <path:value>, --override <path:value>

    Configuration overrides that will be set after the configuration has been loaded.

    Multiple overrides may be specified by using multiple ``--overrides``.

    Example::
    
        piped --override=web.site_name.port:8080

    .. seealso::

        :ref:`piped-configuration-overrides` for more details.


Examples
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Running piped as a non-daemon process::

    piped -n --conf piped.conf

As a non-daemon process with more detailed tracebacks::

    piped -n --conf piped.conf -D



.. _piped-configuration-overrides:

Using command line overrides
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A configuration file can have selected :term:`paths <key_path>` overridden by using :option:`piped -O`.

Each override is a simple dictionary where the keys are :term:`paths <key_path>` and the values are
YAML-serialized values.


.. highlight:: yaml

Given the following configuration::

    foo:
        bar:
            baz: 42
        zip: zap

applying this override::

    foo.bar:{key:value}

results in the following overridden configuration::

    foo:
        bar:
            key: value
        zip: zap

Note that ``foo.bar`` is completely replaced by the override because overrides are not
merged with the configuration, but replaces it.