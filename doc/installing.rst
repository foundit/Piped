Installing Piped
================

Official releases can be installed from PYPI, and development versions are available from `GitHub <http://github.com/foundit/Piped>`_.



A note about dependencies
-------------------------

While Piped itself is written in pure Python, some of the dependencies have C-extensions that needs to be built for each
individual platform. We do not yet have these prepackaged.


Windows users should be able to locate pre-built binaries from http://www.lfd.uci.edu/~gohlke/pythonlibs/ and
http://www.voidspace.org.uk/python/modules.shtml.


Linux users should setup a working build environment, which depends on their distribution. For Debian and Ubuntu
users this is done by installing the ``build-essential`` and ``python-dev`` packages via the package manager.


OS X users should install the `XCode Developer Tools <http://developer.apple.com/technologies/tools/>`_ before
installing Piped.


Installing from PYPI
--------------------

To install Piped from PYPI, use ``easy_install`` or ``pip``. This also installs the Piped scripts in your default interpreters scripts directory.

.. code-block:: bash

    $ easy_install piped

The optional contrib packages are installed the same way:

.. code-block:: bash

    $ easy_install piped.contrib.<contrib_package_name>



Installing a development version
--------------------------------

The latest development version can be downloaded from `GitHub <http://github.com/foundit/Piped>`_:

.. code-block:: bash

    $ git clone git://github.com/foundit/Piped.git


If you do not have git installed, the latest commit can be downloaded a either a .zip or as
a .tar.gz package:

- .zip: https://github.com/foundit/Piped/zipball/develop
- .tar.gz: https://github.com/foundit/Piped/tarball/develop



.. _topic-installing-buildout:

Using buildout
^^^^^^^^^^^^^^

If you have downloaded a development version, you can use `Buildout <http://www.buildout.org/>`_
to create an isolated development environment for Piped:

.. code-block:: bash

    $ cd path/to/the/source
    # bootstrap with the python interpreter you want to use:
    $ python bootstrap.py
    # run the buildout:
    $ bin/buildout
    # eat strawberries while buildout downloads all dependencies

When buildout completes, the Piped scripts are placed in the ``bin/`` folder, alongside the
``buildout`` script.


Required dependencies
"""""""""""""""""""""

Some contrib packages may require dependencies that is not possible for buildout to resolve,
such as :mod:`piped.contrib.zmq`, which depends on ``libzmq`` being available from the operating
system.

When using buildout, all the contrib packages are built by default. If you want to skip some or all of these,
edit ``buildout.cfg`` and comment out the packages you want to skip from the ``[piped-contrib]`` section.
Remember to comment out the package from both the ``develop`` and the ``eggs`` directives.



Running the test suite
----------------------

The test suite can be run by using ``trial``:

.. code-block:: bash

    $ trial piped

If you are using a :ref:`buildout <topic-installing-buildout>` with a development version, use
``piped-trial`` instead, as it takes care of setting up the correct ``sys.path`` before running:


.. code-block:: bash

    $ bin/piped-trial piped