Piped documentation
===================

.. container:: float-right

    .. digraph:: frontpage

        rankdir = LR

        a -> b
        b -> c
        b -> d
        c -> e
        d -> e
        d -> f
        f -> b
        f -> g

Piped is a `MIT-licensed <https://github.com/foundit/Piped/blob/develop/LICENSE>`_ framework for
`flow based programming <http://en.wikipedia.org/wiki/Flow-based_programming>`_ written in Python that focuses on:

* Ease of use.
* Extendability.
* Painless integrating with other systems.
* Testing and maintainability.
* Performance.


A base Piped installation already speaks multiple protocols, such as HTTP, SMTP and Perspective Broker. Contrib packages that extends Piped,
adding support for database connectivity, message queues such as `ZeroMQ <http://zeromq.org>`_ and more are also available.

`Found IT <http://found.no>`_ is currently writing search related extensions to Piped, which will be released as open source soon.

Lets get started:


Taking the first steps
----------------------

You can install official releases from PYPI or development versions from GitHub. Detailed
:doc:`installation instructions <installing>` are available, but the short version is:

.. code-block:: bash

    $ easy_install piped

The :doc:`tutorials/getting_started/index` tutorial is a good introduction to many important aspects of Piped, and is
a recommended read for anyone new to the project.

If you just want to have a peek at how ``hello world`` looks in Piped, here is a sample web server that uses a pipeline:

.. code-block:: yaml

    web:
        first_step:
            routing:
                __config__:
                    pipeline: hello

    pipelines:
        hello:
            - write-web-response:
                fallback_content: Piped says hello.


Learning more
-------------

Piped comes with :doc:`multiple tutorials <tutorials/index>` that focuses on different parts of Piped. Each of the
tutorials focuses on different parts of Piped and teaches how to accomplish a lot of common tasks in Piped.

If you liked the tutorials, take a look at the :doc:`topic guides <topic/index>`. Each topic guide is a good introduction
to different parts of Piped.

Piped comes with many built-in

    - :doc:`Processors <reference/processors>` that processes :term:`batons <baton>` in pipelines.

    - :doc:`Providers <reference/providers>` that makes different resources available to pipelines.

If the processors and providers listed above is insufficient, extending Piped by
:doc:`writing new processors and providers <topic/plugins>` is easy.


Digging deeper
--------------


The :doc:`reference documentation <reference/index>` provides a detailed view into the classes and functions that makes
your process flow.


Feedback and contact
--------------------

Report bugs with Piped in the issue tracker: https://github.com/foundit/Piped/issues. Patches and contributions are always welcome.

Ask questions about Piped on IRC (#piped on `freenode <http://freenode.net/>`_).

Mailing lists are available at piped@librelist.com. Just send an email to the list to subscribe.


.. toctree::
    :hidden:
    :maxdepth: 2

    installing

    tutorials/index

    topic/index

    reference/index

    glossary
