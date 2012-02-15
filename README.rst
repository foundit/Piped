Piped: a framework and application server with pipelines
========================================================

Piped is an `MIT-licensed <https://github.com/foundit/Piped/blob/develop/LICENSE>`_ framework and
application server with built-in support for
`flow based programming <http://en.wikipedia.org/wiki/Flow-based_programming>`_ written in Python that focuses on:

* Extendability.
* Painless integrating with other systems.
* Testing and maintainability.
* Performance.
* Ease of use.

Being based on `Twisted http://twistedmatrix.com/`, a base Piped installation already speaks multiple protocols, such as HTTP, SMTP and Perspective Broker. Contrib packages that extend Piped are also available.


Documentation
-------------

The documentation is available at http://piped.io.


Protocols and Extensions
------------------------

* HTTP/HTTPS with `twisted.web <http://twistedmatrix.com/trac/wiki/TwistedWeb>`_ or `Cyclone <http://cyclone.io>`_ (based on `Tornado <http://tornadoweb.org>`).
* SMTP with `twisted.mail <http://twistedmatrix.com/trac/wiki/TwistedMail>`_
* SSH with `twisted.conch <http://twistedmatrix.com/trac/wiki/TwistedConch>`_
* `ZeroMQ <http://www.zeromq.org/>`_ and `AMQP <http://www.amqp.org/>`_ (i.e `RabbitMQ <http://www.rabbitmq.com/>`_)
* `ZooKeeper <http://zookeeper.apache.org/>`_
* Database-connectivity (with `SQLAlchemy <http://sqlalchemy.org>`_)
* Data-validation (with `FormEncode <http://www.formencode.org/>`_)


Installing and testing
----------------------

See the installation instructions at http://www.piped.io/installing/


Contributing
------------

Report issues in the `issue tracker <https://github.com/foundit/Piped/issues>`_.

Mailing lists are available at piped@librelist.com. Just send an email to the list to subscribe.

Or ask questions about Piped on IRC (#piped on `freenode <http://freenode.net/>`_).
