Using batons
============

A baton is any Python object that is processed in a :doc:`pipeline <pipelines>`.


What happens to a baton during processing is entirely up to the processors: a processor may **replace** the
baton with a new one, or **add**, **update** or **remove** keys in a :ref:`mutable baton <topic-batons-mutable>`.



Baton types
-----------


There are two kinds of batons: *mutable* and *immutable* batons, which is described in the
following sections.

.. _topic-batons-mutable:

Mutable batons
^^^^^^^^^^^^^^

A mutable baton is any Python object that is mutable -- for example :class:`dict`\s instances are the most commonly used
batons.

When using mutable batons in a processor graph containing multiple sources or trees, keep in mind
that all the consumers receives the same baton, and thus will see each others changes. If this is
undesirable, a processor such as :ref:`copy` may be used.




Immutable batons
^^^^^^^^^^^^^^^^

Python objects that are immutable, such as :class:`str`\s may be used by some providers such as the
:mod:`zmq provider <piped_zmq.provider>`.


Processor interdependencies
---------------------------

Processors in a pipeline receive the output baton of the previous processor as its input.

Since any processor may change the baton in any way it sees fit, this creates an interdependency
between batons in a pipeline, as processors later in a processor graph often depend on a baton
conforming to a certain format or that certain values have been set in a dict-like baton.