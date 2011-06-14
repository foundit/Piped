Resources
=========

.. currentmodule:: piped.resource

Interfaces
----------

.. autointerface:: IResourceProvider
    :members:

Classes
-------

.. automodule:: piped.resource



.. autoclass:: ResourceManager

Registering providers
^^^^^^^^^^^^^^^^^^^^^^^^

Providers are registered by calling :meth:`ResourceManager.register`:

.. automethod:: ResourceManager.register


Getting provided resources
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: This is automatically performed by all unresolved :class:`.dependencies.ResourceDependency`
    in :meth:`~.dependencies.ResourceDependency.resolve_initial_state`
    when :meth:`~.dependencies.DependencyManager.resolve_initial_states` is called.

.. automethod:: ResourceManager.resolve