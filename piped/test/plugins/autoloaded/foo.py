# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted import plugin
from zope import interface


class Foo(object):
    interface.classProvides(plugin.IPlugin)
    name = 'foo'