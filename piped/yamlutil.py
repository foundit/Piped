# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
""" Utilities for working with yaml files.

After having imported this module, yaml will have two additional
constructors: ``!filepath`` and ``!pipedpath``.

``!filepath`` constructs `twisted.python.filepath.FilePath` instance with
a path that can be either absolute or relative. If the path is relative,
it is relative to the current working directory of the python process.

``!pipedpath`` constructs a `twisted.python.filepath.FilePath` instance
with a path that is relative to the root of the piped source, and is
often used for including configuration mix-ins that is part of the
piped distribution.
"""
import os
import types

import yaml
from twisted.python import filepath, reflect

import piped
from piped import util


def filepath_constructor(loader, node):
    path = loader.construct_python_str(node)
    fp = filepath.FilePath(util.expand_filepath(path))

    # add a marker to dump the configuration
    fp.origpath = path
    return fp
yaml.add_constructor(u'!filepath', filepath_constructor)


def is_package(any):
    if isinstance(any, types.ModuleType) and '__init__' in any.__file__:
        return True
    return False

def pipedpath_constructor(loader, node):
    path = loader.construct_python_str(node)
    original_path = path
    package = piped
    
    # the path may be an empty string, which should be the root piped package path.
    if path:
        paths = path.split(os.path.sep)

        for i in range(1, len(paths)+1):
            package_name = '.'.join(['piped'] + paths[:i])

            try:
                any = reflect.namedAny(package_name)
            except (AttributeError, reflect.ObjectNotFound) as e:
                # AttributeErrors may occur if we look a file that has the
                # same prefix as an existing module or package
                break

            # we don't want to start looking into modules:
            if not is_package(any):
                break
            package = any

            # update the remaining path
            path = os.path.sep.join(paths[i:])

    root = filepath.FilePath(package.__file__).parent()
    fp = root.preauthChild(util.expand_filepath(path))

    # add markers for dumping the configuration
    fp.origpath = original_path
    fp.pipedpath = True
    return fp

yaml.add_constructor(u'!pipedpath', pipedpath_constructor)


def filepath_representer(dumper, path):
    """ Represents the filepath when dumping. """
    if hasattr(path, 'origpath'):
        if getattr(path, 'pipedpath', False):
            # this was loaded via !pipedpath, so dump it as such
            return dumper.represent_scalar(u'!pipedpath', path.origpath)
        else:
            # we have the original path used when loading, so use it.
            return dumper.represent_scalar(u'!filepath', path.origpath)

    # previously unkonwn path, so we use the full path
    return dumper.represent_scalar(u'!filepath', path.path)

yaml.add_representer(filepath.FilePath, filepath_representer)


class Alias(dict):
    """ An alias for another configuration key.

    This is used in the configuration files to reference other parts of
    the configuration, which may be outside the scope of a single YAML
    file.

    .. seealso:: :class:`YAMLAlias`
    """
    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return 'Alias(%s)'%(self.path)


class YAMLAlias(object):
    """ YAML support for the !alias constructor.

    .. seealso:: :ref:`alias-constructor`
    """
    yaml_tag = u'!alias:'

    @classmethod
    def from_yaml(cls, loader, suffix, node):
        alias = Alias(suffix)
        if isinstance(node, yaml.MappingNode):
            defaults = loader.construct_mapping(node)
            alias.update(defaults)
        return alias

    @classmethod
    def to_yaml(cls, dumper, data):
        return dumper.represent_scalar(cls.yaml_tag+data.name, u'')

yaml.add_representer(Alias, YAMLAlias.to_yaml)
yaml.add_multi_constructor(YAMLAlias.yaml_tag, YAMLAlias.from_yaml)


class BatonPath(str):
    """ YAML support for the !path constructor.

    This is used by :meth:`piped.processors.base.Processor.get_input` to distinguish
    input values that are inlined in the configuration from input values that are found
    at the specified path in the baton.

    .. seealso::
        The topic page for this tag: :ref:`path-constructor`
    """
    yaml_tag = u'!path'

    @classmethod
    def from_yaml(cls, loader, node):
        scalar = loader.construct_scalar(node)
        return cls(scalar)

    @classmethod
    def to_yaml(cls, dumper, data):
        return dumper.represent_scalar(cls.yaml_tag, data)

yaml.add_representer(BatonPath, BatonPath.to_yaml)
yaml.add_constructor(BatonPath.yaml_tag, BatonPath.from_yaml)