Using dependencies
==================

.. currentmodule:: piped.dependencies

Dependencies are used to allow components to react to the availability or unavailability of other components in Piped.



.. _topic-dependencies-lifecycle:

Life cycle of a dependency
--------------------------

* Created
* Added to a dependency graph
* Resolved for the first time
* Changing states ready <-> lost
* Removed from a dependency graph




 
Dependency graph
----------------

The dependency manager adds the dependencies to a dependency graph. A dependency graph is a directed,
acyclic graph (DAG) of :class:`IDependency` instances.

Whenever a dependency fires it ``on_ready`` or ``on_lost`` event, the dependency manager find the
consumers of the dependency in the dependency graph and propagates is to the consumers
``on_dependency_ready`` or ``on_dependency_lost`` event accordingly.

The default :class:`IDependency` implementations calls the ``on_ready`` event
when all of their dependencies are available (or the ``on_lost`` event when one of its
dependencies are lost).

This results in the cascading of dependency availabilities. Consider the following dependency
graph:

.. digraph:: dependency_graph

    rankdir=BT;

    c -> b -> a [color="darkgreen"]
    c -> f [color="darkgreen"]
    d -> b [color="darkgreen"]
    d -> e [color="darkgreen"]
    f -> a [color="darkgreen"]

    a [fontcolor=darkgreen]
    b [fontcolor=darkgreen]
    c [fontcolor=darkgreen]
    d [fontcolor=darkgreen]
    e [fontcolor=darkgreen]
    f [fontcolor=darkgreen]


If ``d`` becomes becomes unavailable, the graph would become:

.. digraph:: dependency_graph_d_lost

    rankdir=BT;

    c -> b [color="darkgreen"]
    c -> f [color="darkgreen"]
    f -> a [color="darkgreen"]
    b -> a [color="red"]
    d -> b [color="red"]
    d -> e [color="red"]

    a [fontcolor=red]
    b [fontcolor=red]
    c [fontcolor=darkgreen]
    d [fontcolor=red]
    e [fontcolor=red]
    f [fontcolor=darkgreen]


Whenever ``d`` becomes available again, the ``on_ready`` events will cascade similarly, and the
dependency graph will return to its former state.




.. _topic-dependencies-resolving:

Resolving dependencies
----------------------

Before a dependency can be used and before its ``on_ready`` and ``on_lost`` are cascaded to its
consumers, it needs to be *resolved*.

When a dependency is resolved, it determines its initial state and fires either its ``on_ready``
or ``on_lost`` event. When the dependency resolves, it becomes eligible to receive events from
its own dependencies and is enable to react to these.




Reacting on dependency availability
-----------------------------------

In order to react to the availability of dependencies, :class:`IDependency` defines two events,
``on_ready``, ``on_lost``.
