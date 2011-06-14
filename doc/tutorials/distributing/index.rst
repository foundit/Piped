Creating a distributed service
==============================


The problem
-----------

We will solve a variant `Project Euler problem #6 <http://projecteuler.net/index.php?section=problems&id=6>`_ using
Piped. The difference between the orignal problem and our variant is in bold:

    The sum of the squares of the first ten natural numbers is,

    1 :sup:`2` + 2 :sup:`2` + ... + 10 :sup:`2` = 385
    The square of the sum of the first ten natural numbers is,

    (1 + 2 + ... + 10) :sup:`2` = 55 :sup:`2` = 3025
    Hence the difference between the sum of the squares of the first ten natural numbers and the square of the sum is 3025 - 385 = 2640.

    Find the difference between the sum of the squares of the **list of input numbers** and the square of the sum.

First, we'll start by solving it in a single process. Then we'll show how to distribute the calculations to
worker nodes write the responses back to the client.


Solving using a single process
------------------------------

.. literalinclude:: 1/rpc-server.yaml
    :language: yaml

Run this configuration:

.. code-block:: bash

    $ piped -nc rpc-server.yaml
    2011-05-30 21:03:38+0200 [-] Log opened.
    2011-05-30 21:03:38+0200 [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    2011-05-30 21:03:38+0200 [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    2011-05-30 21:03:38+0200 [-] twisted.web.server.Site starting on 8080
    2011-05-30 21:03:38+0200 [-] Starting factory <twisted.web.server.Site instance at 0x102b72908>

By opening http://localhost:8080/?n=1&n=2&n=3, we can verify that the sum, square and difference is as expected.

Testing the service
"""""""""""""""""""

Testing a service manually after every configuration change is both time-consuming and burdensome.

.. literalinclude:: 1/test-rpc.yaml
    :language: yaml

The testing pipelines use a processor named ``test-rpc`` which we create in the ``rpc_tutorial`` package:

.. code-block:: bash

    $ mkdir rpc_tutorial
    $ touch rpc_tutorial/__init__.py

Create ``rpc_tutorial/test_rpc.py`` with the following contents:

.. literalinclude:: 1/rpc_tutorial/test_rpc.py


We can now test the service by running the ``test-rpc.yaml`` configuration:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc test-rpc.yaml
    2011-05-30 21:49:20+0200 [-] Log opened.
    2011-05-30 21:49:20+0200 [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    2011-05-30 21:49:20+0200 [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    2011-05-30 21:49:20+0200 [-] twisted.web.server.Site starting on 8088
    2011-05-30 21:49:20+0200 [-] Starting factory <twisted.web.server.Site instance at 0x102e16e60>
    2011-05-30 21:49:21+0200 [status_test] rpc_tutorial.test_rpc
    2011-05-30 21:49:21+0200 [status_test]   TestRPC
    2011-05-30 21:49:21+0200 [HTTPChannel,0,127.0.0.1]     statustest_single_number ... 127.0.0.1 - - [30/May/2011:19:49:20 +0000] "GET /?n=42 HTTP/1.0" 200 40 "-" "Twisted PageGetter"
    2011-05-30 21:49:21+0200 [status_test]                                           [OK]
    2011-05-30 21:49:21+0200 [HTTPChannel,1,127.0.0.1]     statustest_ten_numbers ... 127.0.0.1 - - [30/May/2011:19:49:20 +0000] "GET /?n=1&n=2&n=3&n=4&n=5&n=6&n=7&n=8&n=9&n=10 HTTP/1.0" 200 42 "-" "Twisted PageGetter"
    2011-05-30 21:49:21+0200 [status_test]                                             [OK]
    2011-05-30 21:49:21+0200 [status_test]
    2011-05-30 21:49:21+0200 [status_test] -------------------------------------------------------------------------------
    2011-05-30 21:49:21+0200 [status_test] Ran 2 tests in 0.010s
    2011-05-30 21:49:21+0200 [status_test]
    2011-05-30 21:49:21+0200 [status_test] PASSED (successes=2)
    2011-05-30 21:49:21+0200 [-] (TCP Port 8088 Closed)
    2011-05-30 21:49:21+0200 [-] Stopping factory <twisted.web.server.Site instance at 0x102e16e60>
    2011-05-30 21:49:21+0200 [-] Main loop terminated.
    2011-05-30 21:49:21+0200 [-] Server Shut Down.



Distributing
------------


Decide on a communication protocol.

Getting ZeroMQ
""""""""""""""

Varies by platform:

Windows
^^^^^^^

Download the appropiate ZeroMQ binary from http://www.lfd.uci.edu/~gohlke/pythonlibs/#pyzmq.

OS X
^^^^

The easiest way to install ZeroMQ on OS X is by using `Homebrew <http://mxcl.github.com/homebrew/>`_

.. code-block:: bash

    $ brew install zeromq
    $ easy_install pyzmq

Linux
^^^^^

Follow the installation instructions for zeromq on http://www.zeromq.org/intro:get-the-software, then
easy_install pyzmq:

.. code-block:: bash

    $ easy_install pyzmq


Quick primer on message queues
""""""""""""""""""""""""""""""

We're going to use two four of queues: PUSH, PULL, SUB and PUB.

Push to workers that publishes back to the server.

ZeroMQ sends strings, not structured data, so we have to serialize the messages both to and from the workers.

After getting a response from a worker, we need to see if the request can be finished. This means
we have to store the request in-memory from the time the client makes it until we're ready to
respond.


The workflow
""""""""""""

Below is a diagram that illustrates the workflow of our distributed service.

.. digraph:: distributing_workflow

    pack = false;
    
    client [shape=box];

    client -> receive_request;
    write_response -> client [constraint=false];

    subgraph cluster_worker {
        label = "Worker";
        worker_sum_in -> sum -> worker_sum_out
        worker_square_in -> square -> worker_square_out
    }

    subgraph cluster_server {
        label = "Server";

        receive_request -> compute

        compute -> server_sum_out
        compute -> server_square_out

        server_sum_in -> receive_partial
        server_square_in -> receive_partial

        receive_partial -> write_response

        { rank = same; receive_request; write_response; }

    }

    server_sum_out [shape=invhouse];
    worker_sum_out [shape=invhouse];
    server_square_out [shape=invhouse];
    worker_square_out [shape=invhouse];

    server_sum_in [shape=invhouse];
    worker_sum_in [shape=invhouse];
    server_square_in [shape=invhouse];
    worker_square_in [shape=invhouse];

    server_sum_out -> worker_sum_in [tailport=s, headport=n, weight=1];
    server_square_out -> worker_square_in [tailport=s, headport=n];

    worker_sum_out -> server_sum_in [tailport=s, headport=n];
    worker_square_out -> server_square_in [tailport=s, headport=n];
    

Implementing workers
""""""""""""""""""""

We start by implementing a worker that is going to perform both the calculations. This worker
requires four queues: two input queues and two output queues.

The worker will receive json-encoded messages from the input queues and write a json-encoded
response to the corresponding output queue.

We implement a square client in ``rpc-client-square.yaml``:

.. literalinclude:: 2/rpc-client-square.yaml
    :language: yaml

... a sum client in  ``rpc-client-sum.yaml``:

.. literalinclude:: 2/rpc-client-sum.yaml
    :language: yaml

... and for convenince, we create a configuration that incorporates both these workers into one
single worker in  ``rpc-client.yaml``:

.. literalinclude:: 2/rpc-client.yaml
    :language: yaml


A reader with a keen eye will notice that we've moved the zmq queue definitions to its own file,
which makes it easier. Even if a queue is defined, it will not be created before it is required,
so it is safe for multiple processes to import the same zmq queue definitions. The contents of
``queues.yaml`` should be as follows:

.. literalinclude:: 2/queues.yaml
    :language: yaml


The ``zmq.sum`` and ``zmq.square`` pipelines expect to receive a json-encoded baton on the form:

.. code-block:: js

    {
        id: id,
        numbers: [a, b, c]
    }


... and will respond with a json-encoded baton:

.. code-block:: js

    {
        id: id,
        numbers: [a, b, c],
        sum/square: ...,
    }

Testing the worker
^^^^^^^^^^^^^^^^^^

First we create the ``test-client`` test processor in ``rpc_tutorial/test_client.py``:

.. literalinclude:: 2/rpc_tutorial/test_client.py


Add an entry for ``rpc-client.yaml`` in the list of includes in ``test-rpc.yaml`` and
add the ``test-client`` test processor to the testing pipeline:

.. literalinclude:: 2/test-rpc.yaml
    :end-before: plugins
    :language: yaml

...

.. literalinclude:: 2/test-rpc.yaml
    :start-after: # add any test-processors to this test-pipeline
    :language: yaml

Running the test configuration should now print something like this:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc test-rpc.yaml
    2011-05-31 13:28:47+0200 [-] Log opened.
    2011-05-31 13:28:47+0200 [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    2011-05-31 13:28:47+0200 [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    2011-05-31 13:28:47+0200 [-] /Users/username/pydev/Piped/piped/conf.py:25: piped.exceptions.ConfigurationWarning:
        WARNING: configuration file loaded multiple times: "queues.yaml"
    2011-05-31 13:28:47+0200 [-] twisted.web.server.Site starting on 8080
    2011-05-31 13:28:47+0200 [-] Starting factory <twisted.web.server.Site instance at 0x102989bd8>
    2011-05-31 13:28:48+0200 [status_test] rpc_tutorial.test_rpc
    2011-05-31 13:28:48+0200 [status_test]   TestRPC
    2011-05-31 13:28:48+0200 [-]     statustest_single_number ... 127.0.0.1 - - [31/May/2011:11:28:47 +0000] "GET /?n=42 HTTP/1.0" 200 40 "-" "Twisted PageGetter"
    2011-05-31 13:28:48+0200 [status_test]                                           [OK]
    2011-05-31 13:28:48+0200 [-]     statustest_ten_numbers ... 127.0.0.1 - - [31/May/2011:11:28:47 +0000] "GET /?n=1&n=2&n=3&n=4&n=5&n=6&n=7&n=8&n=9&n=10 HTTP/1.0" 200 42 "-" "Twisted PageGetter"
    2011-05-31 13:28:48+0200 [status_test]                                             [OK]
    2011-05-31 13:28:48+0200 [status_test] rpc_tutorial.test_client
    2011-05-31 13:28:48+0200 [status_test]   TestClient
    2011-05-31 13:28:48+0200 [status_test]     statustest_simple_square ...                                           [OK]
    2011-05-31 13:28:48+0200 [status_test]     statustest_simple_sum ...                                              [OK]
    2011-05-31 13:28:48+0200 [status_test]
    2011-05-31 13:28:48+0200 [status_test] -------------------------------------------------------------------------------
    2011-05-31 13:28:48+0200 [status_test] Ran 4 tests in 0.087s
    2011-05-31 13:28:48+0200 [status_test]
    2011-05-31 13:28:48+0200 [status_test] PASSED (successes=4)
    2011-05-31 13:28:48+0200 [-] (TCP Port 8080 Closed)
    2011-05-31 13:28:48+0200 [-] Stopping factory <twisted.web.server.Site instance at 0x102989bd8>
    2011-05-31 13:28:48+0200 [-] Main loop terminated.
    2011-05-31 13:28:48+0200 [-] Server Shut Down.


Making the server use workers
"""""""""""""""""""""""""""""

In this section, we will extend our rpc-server to use workers to perform its computations.

The workers operates independently of our server, so we have to handle partial responses
from the workers before writing the finished response to the client.


.. literalinclude:: 3/rpc-server.yaml
    :language: yaml

We can verify that this implementation works the same way as the previous one by running our system tests.



Extending the server
--------------------

Adding timeouts
"""""""""""""""

If a server has no workers or messages to or from the workers are lost, a client may be waiting for a request
indefinitely. To avoid this, we need to be able to remove requests that have been pending for too long
without being finished.

This is a three step process:

#. Add a timestamp to the pending_baton when its added to the context.
#. Regularily check the pending context for batons that have been pending for too long.
#. If a pending_baton has been pending for too long, remove it from the pending batons context.

The first step can be done by adding a processor to the ``web.compute`` pipeline before the ``exec-code`` processor:

.. literalinclude:: 4/rpc-server.yaml
    :start-after: # add a timestamp to the baton
    :end-before: - exec-code


To regularily check the for old batons in the pending context, we create a tick interval that will generate
batons and process them in a ``reap_pending_batons`` pipeline on a regular interval. We create ``rpc-server-reaper.yaml``
with the following contents:

.. literalinclude:: 4/rpc-server-reaper.yaml
    :language: yaml

Insert the above processor into the ``web.work`` pipeline and append ``rpc-server-reaper.yaml`` to the list of
includes in ``rpc-server.yaml``.


Testing the timeouts
^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: 4/rpc_tutorial/test_rpc.py
    :pyobject: TestRPCWithoutWorkersProcessor


Add the test processor to the testing pipeline:

.. literalinclude:: 4/test-rpc.yaml
    :language: yaml
    :start-after: # add any test-processors to this test-pipeline

Running the tests now should result in output resembling this:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc test-rpc.yaml
    2011-05-31 15:52:35+0200 [-] Log opened.
    2011-05-31 15:52:35+0200 [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    2011-05-31 15:52:35+0200 [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    2011-05-31 15:52:35+0200 [-] /Users/username/pydev/Piped/piped/conf.py:25: piped.exceptions.ConfigurationWarning:
        WARNING: configuration file loaded multiple times: "queues.yaml"
    2011-05-31 15:52:36+0200 [-] twisted.web.server.Site starting on 8080
    2011-05-31 15:52:36+0200 [-] Starting factory <twisted.web.server.Site instance at 0x102e237a0>
    2011-05-31 15:52:36+0200 [status_test] rpc_tutorial.test_rpc
    2011-05-31 15:52:36+0200 [status_test]   TestRPC
    2011-05-31 15:52:36+0200 [-]     statustest_single_number ... 127.0.0.1 - - [31/May/2011:13:52:36 +0000] "GET /?n=42 HTTP/1.0" 200 40 "-" "Twisted PageGetter"
    2011-05-31 15:52:36+0200 [status_test]                                           [OK]
    2011-05-31 15:52:36+0200 [-]     statustest_ten_numbers ... 127.0.0.1 - - [31/May/2011:13:52:36 +0000] "GET /?n=1&n=2&n=3&n=4&n=5&n=6&n=7&n=8&n=9&n=10 HTTP/1.0" 200 42 "-" "Twisted PageGetter"
    2011-05-31 15:52:36+0200 [status_test]                                             [OK]
    2011-05-31 15:52:36+0200 [status_test] rpc_tutorial.test_client
    2011-05-31 15:52:36+0200 [status_test]   TestClient
    2011-05-31 15:52:36+0200 [status_test]     statustest_simple_square ...                                           [OK]
    2011-05-31 15:52:36+0200 [status_test]     statustest_simple_sum ...                                              [OK]
    2011-05-31 15:52:36+0200 [status_test] rpc_tutorial.test_rpc
    2011-05-31 15:52:36+0200 [status_test]   TestRPCWithoutWorkers
    2011-05-31 15:52:39+0200 [-]     statustest_single_number ... 127.0.0.1 - - [31/May/2011:13:52:39 +0000] "GET /?n=42 HTTP/1.0" 200 18 "-" "Twisted PageGetter"
    2011-05-31 15:52:39+0200 [status_test]                                           [OK]
    2011-05-31 15:52:39+0200 [status_test]
    2011-05-31 15:52:39+0200 [status_test] -------------------------------------------------------------------------------
    2011-05-31 15:52:39+0200 [status_test] Ran 5 tests in 3.000s
    2011-05-31 15:52:39+0200 [status_test]
    2011-05-31 15:52:39+0200 [status_test] PASSED (successes=5)
    2011-05-31 15:52:39+0200 [-] (TCP Port 8080 Closed)
    2011-05-31 15:52:39+0200 [-] Stopping factory <twisted.web.server.Site instance at 0x102e237a0>
    2011-05-31 15:52:39+0200 [-] Main loop terminated.
    2011-05-31 15:52:39+0200 [-] Server Shut Down.