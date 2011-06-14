# -*- test-case-name: piped.test.test_graph -*-

# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Graph classes, mostly customizations of networkx-classes. """
import networkx as nx

from piped import util


class DirectedGraphWithOrderedEdges(nx.DiGraph):
    """ Directed graph that preserves the order of nodes added, and
    the order of the edges added between nodes.

    Additional methods not provided by `networkx.DiGraph`:
        - :meth:`replace_edge_from`

    See `networkx.DiGraph` for documentation on the rest. """

    ordered_dictionary_factory = util.OrderedDictionary

    def __init__(self):
        super(DirectedGraphWithOrderedEdges, self).__init__()
        self.adj = self.succ = self.ordered_dictionary_factory()
        self.pred = self.ordered_dictionary_factory()

    def _ensure_ordered_dicts_for_node(self, node):
        self.succ.setdefault(node, self.ordered_dictionary_factory())
        self.pred.setdefault(node, self.ordered_dictionary_factory())
        # The ordering of nodes isn't important. (We iterate over the
        # graph e.g. in BFS or topological ordering, not by the order
        # they're added to the graph. )
        self.node.setdefault(node, dict())

    def add_node(self, node, *a, **kw):
        self._ensure_ordered_dicts_for_node(node)
        super(DirectedGraphWithOrderedEdges, self).add_node(node, *a, **kw)

    def add_nodes_from(self, nodes, **kw):
        for node in nodes:
            self._ensure_ordered_dicts_for_node(node)
        super(DirectedGraphWithOrderedEdges, self).add_nodes_from(nodes, **kw)

    def add_edge(self, u, v, attr_dict=None, **attr):
        self._ensure_ordered_dicts_for_node(u)
        self._ensure_ordered_dicts_for_node(v)
        super(DirectedGraphWithOrderedEdges, self).add_edge(u, v, attr_dict, **attr)

    def add_edges_from(self, ebunch, *a, **kw):
        for u, v, _ in ebunch:
            self._ensure_ordered_dicts_for_node(u)
            self._ensure_ordered_dicts_for_node(v)
        super(DirectedGraphWithOrderedEdges, self).add_edges_from(ebunch, *a, **kw)

    def replace_edge_from(self, from_node, old_to, new_to, **kw):
        """ Replaces the edge (*from_node*, *old_to*) with
        (*from_node*, *new_to*), optionally with the attributes
        provided by the keyword arguments.

        :Exceptions:
            ValueError
                If the existing edge does not exist, or the new edge already exists.
        """
        if not self.has_edge(from_node, old_to):
            raise ValueError('%s has no edge to %s.' % (from_node, old_to))
        if self.has_edge(from_node, new_to):
            # Since this isn't a multi-graph:
            raise ValueError('%s already has an edge to %s.' % (from_node, new_to))

        self._ensure_ordered_dicts_for_node(new_to)
        self.succ[from_node].replace_key(old_to, new_to, kw)
        self.pred[new_to][from_node] = kw
        del self.pred[old_to][from_node]
