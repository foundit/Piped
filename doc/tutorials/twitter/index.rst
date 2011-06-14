Integrating with Twitter
========================


In this tutorial we will create a :ref:`provider <topic-plugin-provider>` for objects that implement the
`Twitter API <http://dev.twitter.com/doc>`_.

We will start by creating a simple provider that supports basic authentication, then add support
for `OAuth <http://dev.twitter.com/pages/auth>`_, which is required to use many of the Twitter API features.

After adding OAuth support, we will add support for consuming streams of statuses from the
`Twitter Streaming API <http://dev.twitter.com/pages/streaming_api>`_ and processing these in
pipelines.

We will create a simple :ref:`processor <topic-plugin-processor>` that uses the provided API to update the
status of an account.





Creating a basic Twitter provider
---------------------------------------

First we need to decide on a configuration format that our provider will use.

We define the following requirements to the configuration format:

    * It should be possible to override and extend an account configuration from an included file. As
      discussed in :ref:`topic-configuration-includes`, this requires us to avoid the use of lists
      in favor of dictionaries.
    * Multiple accounts.
    * One authentication per account.

By looking at the above requirements, we define the following configuration structure:

.. code-block:: yaml

    twitter:
        <logical-account-name>:
            auth:
                username: <my_twitter_username>
                password: <my_twitter_password>


Creating the project structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to interface the twitter APIs with pipelines, we need to write a :ref:`provider plugin <topic-plugin-provider>`.

For piped to be able to find our custom provider, we need to put it in a package, add that package to our ``sys.path``,
then inform piped to look in that package for plugins:

We create the following directory structure:

.. code-block:: bash

    # twitter.yaml        # our configuration file
    # twitter_tutorial/   # our python package
    #     __init__.py     # empty file that makes twitter_provider a package
    #     provider.py     # we'll add our provider code here.

    $ mkdir twitter_tutorial
    $ touch twitter_tutorial/__init__.py


Writing the provider
^^^^^^^^^^^^^^^^^^^^

First, we must decide on which Python library for the Twitter API we should use. By looking at
http://dev.twitter.com/pages/libraries#python, we see that `tweepy <https://github.com/tweepy/tweepy>`_
is favourably mentioned, so we will start by installing it:

.. code-block:: bash

    $ easy_install tweepy

Writing a provider for a third party library requires a certain amount of understanding both of how
the library works and of piped providers.  This tutorial assumes that the reader is somewhat familiar with
the tweepy library, and will focus on how the provider part works.



Create the provider implementation as  ``twitter_tutorial/provider.py``:

.. literalinclude:: 1_basic/twitter_tutorial/provider.py



Using the provider
^^^^^^^^^^^^^^^^^^

Create the configuration file ``twitter.yaml`` (remember to change the username/password):


.. literalinclude:: 1_basic/twitter.yaml
    :language: yaml

Now we can run the pipeline, but we have to make sure that ``twitter_tutorial`` is in pythons ``sys.path``, which
we do by setting the ``PYTHONPATH`` environment variable:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc twitter.yaml
    [-] Log opened.
    [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    [-] "Current rate limit status: {u'reset_time': u'...', ...}"


We've opted to use the :ref:`eval-lambda` processor to look up the rate limit status of the specified account. While
this works great for simple use cases, it is generally recommended to write custom processors that expose functionality
like this. How this is done is covered later in this tutorial.


Adding OAuth support
--------------------

Twitter does not allow full access to the api by using basic authentication. Full access requires an OAuth login, which
is described in detail at http://dev.twitter.com/pages/auth.

After registering your application at https://dev.twitter.com/apps, you will receive a ``consumer_key`` and a ``consumer_token``,
which are used to generate authentication urls that Twitter users can open to grant this application access to their account on
their behalf.

We extend our configuration to include the following:

.. code-block:: yaml

    twitter:
        <logical-account-name>:
            auth:
                # instead of using username/password, we can use:
                consumer_key: <my_consumer_key>
                consumer_secret: <my_consumer_secret>

                access_token:
                    key: <my_access_key>
                    secret: <my_access_secret>


Replace the following code in ``twitter_tutorial/provider.py``::

    auth = tweepy.BasicAuthHandler(**account_config['auth'])

with::

    auth = self._get_auth(account_name, account_config['auth'])

and implement ``_get_auth``:

.. literalinclude:: 2_oauth/twitter_tutorial/provider.py
    :pyobject: MyTwitterProvider._get_auth

.. note:: We have opted to do the OAuth dance as part of our service startup if the ``access_token``
    is not provided by the configuration. ``raw_input`` blocks the reactor, and our application will
    not continue starting until the PID has been entered.

Update ``twitter.my_account.auth`` (removing the previously defined ``username`` and ``password`` keys) in ``twitter.yaml``:

.. literalinclude:: 2_oauth/twitter.yaml
    :start-after: # define a twitter account
    :end-before: system-events
    :language: yaml


And our provider should now be able to authenticate via OAuth, without changing the pipeline contents:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc twitter.yaml
    [-] Log opened.
    [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    [-] "Current rate limit status: {u'reset_time_in_seconds': 1304689529, ...}"


Adding support for the streaming API
------------------------------------

This section shows how to add support for processing Twitter streams in pipelines.

The `Twitter Streaming API <http://dev.twitter.com/pages/streaming_api>`_ allows high-throughput near-realtime access to
various subsets of public and protected Twitter data.

We want to be able to support multiple streams per account so we extend the account configuration with a
``streams`` key. Keeping in mind that we would like as much of the configuration as possible to be includable, we use
the following configuration format for the streams:

.. code-block:: yaml

    twitter:
        <logical-account-name>:
            # OPTIONAL streams that are processed in a pipeline
            streams:
                <logical-stream-name>:
                    method: <stream-method>
                    pipeline: <pipeline-name>
                    # OPTIONAL keyword arguments to use when creating the stream, for example:
                    track:
                        - new



Using services
^^^^^^^^^^^^^^

We're going to run each of the streams as separate threads within our process. To ensure that the streams are started
and stopped along with the rest of the system, we're going to use the built-in support of services in Twisted.

The services hierarchy we're going to create will look like this:

.. digraph:: twitter_services

    "Piped Process (application)" -> "Twitter provider (MultiService)"
    "Twitter provider (MultiService)" -> "Twitter stream listener 1 (MultiService)" -> "Twitter stream 1 (Service)"
    "Twitter provider (MultiService)" -> "Twitter stream listener 2 (MultiService)" -> "Twitter stream 2 (Service)"


We separate the notion of a stream listener and a stream. The stream listener will be a subclass of
:class:`tweepy.StreamListener`. Stream listeners are called by the streams in order to process received status
objects etc. The streams will be run in threads and maintain a connection to the streaming API, parsing incoming
status updates and sending them to its stream listener.

While the stream listeners do not maintain more than one :class:`~twisted.application.service.Service` instance,
subclassing from :class:`~twisted.application.service.MultiService` is required in order to manage one or more
:class:`~twisted.application.service.Service` subclasses.

To do this, we must make our Twitter provider inherit from :class:`twisted.application.service.MultiService`:

.. literalinclude:: 3_streaming/twitter_tutorial/provider.py
    :start-after: from piped import
    :end-before: self.twitter_configs

Since :class:`~twisted.application.service.MultiService` is an old-style class, we cannot use :func:`super` to
call its constructor. We use :class:`~twisted.application.service.Service.setServiceParent` in
:meth:`~piped.resource.IResourceProvider.configure` in order to make the provider automatically start and stop when
the application is started or stopped.



Writing the streaming service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


The first thing we do, is adapting the :class:`tweepy.Stream` to a :class:`~twisted.application.service.Service` that
we can start and stop along with our provider. We do this by creating a class that inherits from both classes in
``twitter_tutorial/provider.py``:


.. literalinclude:: 3_streaming/twitter_tutorial/provider.py
    :pyobject: TwitterStream



Streaming statuses to a pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As previously mentioned, a :class:`tweepy.StreamListener` receives statuses from a :class:`tweepy.Stream`. In our case, we're
going to create the stream in our stream listener subclass and process the statuses in a pipeline.

The stream will call the listeners :meth:`~tweepy.StreamListener.on_status` with a status model every time a complete status
has been received. This call is made from the thread we spawned in :meth:`TwitterStream.startService`, and it is very important
to only invoke pipelines from the Twisted main thread, or processors that rely on features such as
:class:`~twisted.internet.defer.Deferred`\s might not work properly.

To be able to process a status in a pipeline, we must create a dependency from the stream listener to the pipeline. After we've
done that, we can use the returned :class:`~piped.dependencies.ResourceDependency` to get the pipeline instance.

Add the following code to ``twitter_tutorial/provider.py``, after the provider implementation:

.. literalinclude:: 3_streaming/twitter_tutorial/provider.py
    :pyobject: TwitterStreamListener


Since the ``TwitterStreamListener`` is going to act as an input source, we will create instances of it when the provider is
configured:

.. literalinclude:: 3_streaming/twitter_tutorial/provider.py
    :pyobject: MyTwitterProvider.configure


Then we update our ``twitter.yaml`` to add a stream:


.. literalinclude:: 3_streaming/twitter.yaml
    :start-after: # define a twitter account
    :end-before: system-events

The stream needs a pipeline to process the status updates, so we add the following pipeline to our ``pipelines``:

.. literalinclude:: 3_streaming/twitter.yaml
    :start-after: # print the status updates


Running ``piped`` now should result in a continuous stream of tweets being printed:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc twitter.yaml
    [-] Log opened.
    [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    [-] "Current rate limit status: {u'reset_time_in_seconds': 1304690221, ...}"
    [-] u'status-update -> screen_name(114): ...'
    [-] u'status-update -> screen_name(23): ...'
    [-] u'status-update -> screen_name(27): ...'
    [-] u'status-update -> screen_name(229): ...'
    [-] u'status-update -> screen_name(623): ...'
    [-] u'status-update -> screen_name(2583): ...'
    [-] u'status-update -> screen_name(220): ...'
    [-] u'status-update -> screen_name(157): ...'

.. note:: Since both the rate limiting and the stream api is being used in threads, there are no guarantees that the
    "Current rate limit status" message will be printed before any statuses.



Creating a processor that updates the Twitter status
----------------------------------------------------

In this section we will create a processor that should update the status of our Twitter
account.

What a processor is and how it works is explained in further detail in :ref:`topic-plugin-processor`.



Writing the processor
^^^^^^^^^^^^^^^^^^^^^

One of the important decisions when writing a plugin is giving it a logical name. The processor name
is used in the configuration, and should read in such a way that it gives the reader of the configuration
an immediate understanding of what it does, and if possible, how it works.

Since our processor is going to update the status of a twitter account, we will name the processor
``update-twitter-status``.

The processor will be configured with an account name and a path to the status in the baton. When the
processor is configured, we create a :doc:`dependency </topic/dependencies>` to the Twitter API that the
provider has provided.

When the processor processes a baton, it looks up the status in the baton and updates the Twitter accounts
status.

We create the processor in a file called ``twitter_tutorial/processors.py``:


.. literalinclude:: 4_processor/twitter_tutorial/processors.py




Using the processor
^^^^^^^^^^^^^^^^^^^

To demonstrate the use of this processor, we change the ``startup`` pipeline in ``twitter.yaml``:


.. literalinclude:: 4_processor/twitter.yaml
    :language: yaml
    :start-after: pipelines:


We use the :ref:`eval-lambda` processor to create our status message. In a production application, this would
usually be set by something else, possibly the text of another twitter status update.

Running ``piped`` with our ``twitter.yaml`` configuration file should print something like this:

.. code-block:: bash

    $ PYTHONPATH=. piped -nc twitter.yaml
    [-] Log opened.
    [-] twistd 11.0.0 (/Users/username/.virtualenvs/Piped/bin/python2.7 2.7.1) starting up.
    [-] reactor class: twisted.internet.selectreactor.SelectReactor.
    [-] Updated the status of u'TWITTER_USERNAME' to u'Today is 2011-05-06'



Processing tweets and generating content
----------------------------------------

In this section, we're going to create two more processors. The first processor is going to create a Markov chain, and
the other is going to use the Markov chain to generate new Twitter status updates.

A `Markov chain <http://en.wikipedia.org/wiki/Markov_chain>`_ is simply put a system that can be used to generate a new
state based on the current state. In this section, we will create train a Markov chain with a stream of tweets and use
this markov chain to generate new tweets.


This is a Markov chain of length 2 after training with the sentence: "This is a Markov chain"

.. code-block:: yaml

    START, START: this
    START, this: is
    this, is: a
    is, a: Markov
    a, Markov: chain
    Markov, chain: END

After training with "This is another sentence", the model will look like this:

.. code-block:: yaml

    START, START: this
    START, this: is
    this, is: a, another
    is, a: Markov
    a, Markov: chain
    Markov, chain: END
    is, another: sentence
    another, sentence: END

We can use this data structure to generate new sentences by starting with the starting state ``(START, START)`` and picking
a word randomly from the list of possible words for that state until we pick the word ``END``. This may work surprisingly well.


Streaming happy tweets
^^^^^^^^^^^^^^^^^^^^^^

To train our model we will be using the text of incoming tweets. To do this, we will ask Twitter to send us all
status updates that contains the word ``happy``, and update our Markov chain in a processor. The Markov model needs to be
stored in a place where it can be reached by other pipelines and processors, so we use a shared context:

.. literalinclude:: 5_markov/twitter.yaml
    :language: yaml
    :start-after: # use a shared context to store
    :end-before: #


To stream status updates from Twitter containing the word ``happy``, we add the following streams configuration
to our Twitter account configuration in ``twitter.yaml``:

.. literalinclude:: 5_markov/twitter.yaml
    :language: yaml
    :start-after: # track a stream using a pipeline:
    :end-before: # use a shared context to store


Training our model
^^^^^^^^^^^^^^^^^^

Add the following ``train-markov`` processor to ``twitter_tutorial/processors.py``:

.. literalinclude:: 5_markov/twitter_tutorial/processors.py
    :pyobject: MarkovTrainer


Generating twitter statuses
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To generate sentences, we'll be creating a ``generate-markov`` processor, which we also add to ``twitter_tutorial/processors.py``:

.. literalinclude:: 5_markov/twitter_tutorial/processors.py
    :pyobject: MarkovGenerator

Don't forget to add ``import random`` to the top of ``twitter_tutorial/processors.py``:

.. literalinclude:: 5_markov/twitter_tutorial/processors.py
    :end-before: piped
    :append: (...)


Putting it all together
^^^^^^^^^^^^^^^^^^^^^^^

Our pipelines should now look like this:

.. literalinclude:: 5_markov/twitter.yaml
    :language: yaml
    :start-after: pipelines:


Now only one thing remains: making the ``web.generate`` pipeline available through a web interface:

.. literalinclude:: 5_markov/twitter.yaml
    :language: yaml
    :start-after: # make a web frontend:
    :end-before: pipelines:


Start piped and after a while you should be able to generate amazing new tweets by visiting http://localhost:8080/generate.



What's next
-----------

An exercise for the reader is to let the user post interesting generated tweets with a single click after having generated it. This is possible to
accomplish by extending the current web pipeline, or adding an additional one.


To learn more about complex pipeline graphs, see :doc:`/topic/pipelines`.