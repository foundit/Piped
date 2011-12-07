"""
Plugins go in directories on your PYTHONPATH named piped/plugins:
this is the only place where an __init__.py is necessary, thanks to
the __path__ variable.
"""

from twisted.plugin import pluginPackagePaths
__path__.extend(pluginPackagePaths(__name__))
__all__ = []#nothing to see here, move along, move along