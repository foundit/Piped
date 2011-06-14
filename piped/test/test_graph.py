# -*- coding: utf-8 -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import copy

from twisted.trial import unittest

from piped import graph


class TestDirectedGraphWithOrderedEdges(unittest.TestCase):

    def setUp(self):
        self.graph = graph.DirectedGraphWithOrderedEdges()
        # Make a set of stuff which gets an arbitrary order in the set.
        items = set(range(-5, 5) + list(u'abcæøåÆØÅ'))
        # Order the items in a way that is different from how they're
        # ordered in the hash. Taking the reverse should be fine.
        self.ordered_items = list(items)[::-1]

    def assertHasEdge(self, from_, to, msg=None):
        if msg is None:
            msg = 'Expected graph to contain (%s, %s)' % (from_, to)
        self.assertTrue(self.graph.has_edge(from_, to), msg)

    def assertNotHasEdge(self, from_, to, msg=None):
        if msg is None:
            msg = 'Expected graph to NOT contain (%s, %s)' % (from_, to)
        self.assertFalse(self.graph.has_edge(from_, to), msg)

    def test_order_of_nodes_added(self):
        for item in self.ordered_items:
            self.graph.add_node(item)
        self.assertEquals(self.graph.nodes(), self.ordered_items)

    def test_order_of_nodebunch_added(self):
        self.graph.add_nodes_from(self.ordered_items)
        self.assertEquals(self.graph.nodes(), self.ordered_items)

    def test_order_of_edges_added(self):
        reversed_items = self.ordered_items[::-1]
        for a in self.ordered_items:
            for b in reversed_items:
                self.graph.add_edge(a, b)
            self.assertEquals(self.graph.successors(a), reversed_items)

    def test_order_of_edgebunch_added(self):
        reversed_items = self.ordered_items[::-1]
        for a in self.ordered_items:
            # add_edges_from expects a list of tuples on the form (from, to, dict)
            edgebunch = [(a, b, dict()) for b in reversed_items]
            self.graph.add_edges_from(edgebunch)
            self.assertEquals(self.graph.successors(a), reversed_items)

    def test_replacing_first_edge(self):
        self.graph.add_edge('a', 'b')
        self.graph.add_edge('b', 'c')
        self.graph.replace_edge_from(from_node='a', old_to='b', new_to='d')

        self.assertHasEdge('a', 'd')
        self.assertHasEdge('b', 'c')
        self.assertNotHasEdge('a', 'b')

    def test_replacing_last_edge(self):
        self.graph.add_edge('a', 'b')
        self.graph.add_edge('b', 'c')
        self.graph.replace_edge_from(from_node='b', old_to='c', new_to='d')

        self.assertHasEdge('a', 'b')
        self.assertHasEdge('b', 'd')
        self.assertNotHasEdge('b', 'c')

    def test_replacing_only_edge(self):
        self.graph.add_edge('a', 'b')
        self.graph.replace_edge_from(from_node='a', old_to='b', new_to='d')

        self.assertHasEdge('a', 'd')
        self.assertNotHasEdge('a', 'b')

    def test_replacement_errors(self):
        self.assertRaises(ValueError, self.graph.replace_edge_from, 'a', 'b', 'd')

        self.graph.add_edge('a', 'b')
        self.graph.add_edge('a', 'c')

        self.assertRaises(ValueError, self.graph.replace_edge_from, 'a', 'b', 'c')

    def test_deepcopying_graph(self):
        g1 = graph.DirectedGraphWithOrderedEdges()
        g1.add_edge('a', 'c')
        g1.add_edge('a', 'd')
        g1.add_edge('a', 'b')
        self.assertEquals(g1.edges(), [('a', 'c'), ('a', 'd'), ('a', 'b')])

        g2 = copy.deepcopy(g1)
        self.assertEquals(g2.edges(), [('a', 'c'), ('a', 'd'), ('a', 'b')])
        g2.remove_edge('a', 'd')
        self.assertEquals(g1.edges(), [('a', 'c'), ('a', 'd'), ('a', 'b')])
        self.assertEquals(g2.edges(), [('a', 'c'), ('a', 'b')])

    def test_deepcopying_graph_and_nodes(self):

        class Foo(object):
            """ Just something deep-copyable that is hashable, mutable
            and comparable. """

            def __init__(self, value):
                self.value = value

            def __cmp__(self, other):
                return cmp(self.value, other.value)

        a = Foo('a')
        b = Foo('b')
        c = Foo('c')

        g1 = graph.DirectedGraphWithOrderedEdges()
        g1.add_edge(a, b)
        g1.add_edge(a, c)

        self.assertEquals(g1.edges(), [(a, b), (a, c)])

        g2 = copy.deepcopy(g1)
        self.assertEquals(g2.edges(), [(a, b), (a, c)])
        for i, j in zip(g1.nodes(), g2.nodes()):
            self.assertTrue(i == j)
            self.assertFalse(i is j)
