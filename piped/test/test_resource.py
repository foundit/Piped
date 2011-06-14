# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.trial import unittest
from zope import interface

from piped import resource, exceptions


class FakeResourceProvider(object):
    interface.classProvides(resource.IResourceProvider)

    def __init__(self, name):
        self.name = name


class ResourceManagerTest(unittest.TestCase):

    def setUp(self):
        self.resource_manager = resource.ResourceManager()

    def test_invoking_the_right_provider(self):
        a = FakeResourceProvider('a')
        a_b = FakeResourceProvider('a b')

        self.resource_manager.register('a', a)
        self.resource_manager.register('a.b', a_b)
        self.assertRaises(exceptions.ProviderConflict, self.resource_manager.register, 'a', a)
        self.assertRaises(exceptions.ProviderConflict, self.resource_manager.register, 'a', a_b)
        self.assertRaises(exceptions.ProviderConflict, self.resource_manager.register, 'a.b', a_b)

        self.assertEquals(self.resource_manager.get_provider_or_fail('a'), a)
        self.assertEquals(self.resource_manager.get_provider_or_fail('a.b'), a_b)
        self.assertEquals(self.resource_manager.get_provider_or_fail('a.b'), a_b)
        self.assertRaises(exceptions.UnprovidedResourceError, self.resource_manager.get_provider_or_fail, 'c')


__doctests__ = [resource]