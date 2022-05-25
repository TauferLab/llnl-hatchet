# Copyright 2017-2022 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from .function import FunctionQuery
from ..node import Node, traversal_order

from itertools import groupby
import pandas as pd
from pandas.core.indexes.multi import MultiIndex


class QueryEngine:

    def __init__(self):
        # Initialize containers for query and memoization cache.
        self.query_pattern = []
        self.search_cache = {}

    def apply(self, gf, query):
        if isinstance(query, FunctionQuery):
            return self._apply_one(gf, query)
        elif isinstance(query, NaryQuery):
            results = []
            for subquery in query.subqueries:
                results.append(self.apply(gf, subquery))
            return query._perform_nary_op(results, gf)
        else:
            raise TypeError("Unknown query type ({}) provided!".format(type(query)))

    def _apply_one(self, gf, query):
        """Applies a single query to a GraphFrame.

        Arguments:
            gf (GraphFrame): the GraphFrame on which to apply the query.

        Returns:
            (list): A list representing the set of nodes from paths that match this query.
        """
        self.query_pattern = query.query_pattern
        self.search_cache = {}
        matches = []
        visited = set()
        for root in sorted(gf.graph.roots, key=traversal_order):
            self._apply_impl(gf, root, visited, matches)
        assert len(visited) == len(gf.graph)
        matched_node_set = list(set().union(*matches))
        # return matches
        return matched_node_set

    def _cache_node(self, gf, node):
        """Cache (Memoize) the parts of the query that the node matches.

        Arguments:
            gf (GraphFrame): the GraphFrame containing the node to be cached.
            node (Node): the Node to be cached.
        """
        assert isinstance(node, Node)
        matches = []
        # Applies each filtering function to the node to cache which
        # query nodes the current node matches.
        for i, node_query in enumerate(self.query_pattern):
            _, filter_func = node_query
            row = None
            if isinstance(gf.dataframe.index, MultiIndex):
                row = pd.concat([gf.dataframe.loc[node]], keys=[node], names=["node"])
            else:
                row = gf.dataframe.loc[node]
            if filter_func(row):
                matches.append(i)
        self.search_cache[node._hatchet_nid] = matches

    def _match_0_or_more(self, gf, node, wcard_idx):
        """Process a "*" wildcard in the query on a subgraph.

        Arguments:
            gf (GraphFrame): the GraphFrame being queried.
            node (Node): the node being queried against the "*" wildcard.
            wcard_idx (int): the index associated with the "*" wildcard query.

        Returns:
            (list): a list of lists representing the paths rooted at "node" that match the "*" wildcard and/or the next query node. Will return None if there is no match for the "*" wildcard or the next query node.
        """
        # Cache the node if it's not already cached
        if node._hatchet_nid not in self.search_cache:
            self._cache_node(gf, node)
        # If the node matches with the next non-wildcard query node,
        # end the recursion and return the node.
        if wcard_idx + 1 in self.search_cache[node._hatchet_nid]:
            return [[]]
        # If the node matches the "*" wildcard query, recursively
        # apply this function to the current node's children. Then,
        # collect their returned matches, and prepend the current node.
        elif wcard_idx in self.search_cache[node._hatchet_nid]:
            matches = []
            if len(node.children) == 0:
                if wcard_idx == len(self.query_pattern) - 1:
                    return [[node]]
                return None
            for child in sorted(node.children, key=traversal_order):
                sub_match = self._match_0_or_more(gf, child, wcard_idx)
                if sub_match is not None:
                    matches.extend(sub_match)
            if len(matches) == 0:
                return None
            tmp = set(tuple(m) for m in matches)
            matches = [list(t) for t in tmp]
            return [[node] + m for m in matches]
        # If the current node doesn't match the current "*" wildcard or
        # the next non-wildcard query node, return None.
        else:
            if wcard_idx == len(self.query_pattern) - 1:
                return [[]]
            return None

    def _match_1(self, gf, node, idx):
        """Process a "." wildcard in the query on a subgraph.

        Arguments:
            gf (GraphFrame): the GraphFrame being queried.
            node (Node): the node being queried against the "." wildcard.
            wcard_idx (int): the index associated with the "." wildcard query.

        Returns:
            (list): A list of lists representing the children of "node" that match the "." wildcard being considered. Will return None if there are no matches for the "." wildcard.
        """
        if node._hatchet_nid not in self.search_cache:
            self._cache_node(gf, node)
        matches = []
        for child in sorted(node.children, key=traversal_order):
            # Cache the node if it's not already cached
            if child._hatchet_nid not in self.search_cache:
                self._cache_node(gf, child)
            if idx in self.search_cache[child._hatchet_nid]:
                matches.append([child])
        # To be consistent with the other matching functions, return
        # None instead of an empty list.
        if len(matches) == 0:
            return None
        return matches

    def _match_pattern(self, gf, pattern_root, match_idx):
        """Try to match the query pattern starting at the provided root node.

        Arguments:
            gf (GraphFrame): the GraphFrame being queried.
            pattern_root (Node): the root node of the subgraph that is being queried.

        Returns:
            (list): A list of lists representing the paths rooted at "pattern_root" that match the query.
        """
        assert isinstance(pattern_root, Node)
        # Starting query node
        pattern_idx = match_idx + 1
        if self.query_pattern[match_idx][0] == "*":
            pattern_idx = 0
        # Starting matching pattern
        matches = [[pattern_root]]
        while pattern_idx < len(self.query_pattern):
            # Get the wildcard type
            wcard, _ = self.query_pattern[pattern_idx]
            new_matches = []
            # Consider each existing match individually so that more
            # nodes can be added to them.
            for m in matches:
                sub_match = []
                # Get the portion of the subgraph that matches the next
                # part of the query.
                if wcard == ".":
                    s = self._match_1(gf, m[-1], pattern_idx)
                    if s is None:
                        sub_match.append(s)
                    else:
                        sub_match.extend(s)
                elif wcard == "*":
                    if len(m[-1].children) == 0:
                        sub_match.append([])
                    else:
                        for child in sorted(m[-1].children, key=traversal_order):
                            s = self._match_0_or_more(gf, child, pattern_idx)
                            if s is None:
                                sub_match.append(s)
                            else:
                                sub_match.extend(s)
                else:
                    raise InvalidQueryFilter(
                        'Query wildcards must (internally) be one of "." or "*"'
                    )
                # Merge the next part of the match path with the
                # existing part.
                for s in sub_match:
                    if s is not None:
                        new_matches.append(m + s)
                new_matches = [uniq_match for uniq_match, _ in groupby(new_matches)]
            # Overwrite the old matches with the updated matches
            matches = new_matches
            # If all the existing partial matches were not able to be
            # expanded into full matches, return None.
            if len(matches) == 0:
                return None
            # Update the query node
            pattern_idx += 1
        return matches

    def _apply_impl(self, gf, node, visited, matches):
        """Traverse the subgraph with the specified root, and collect all paths that match the query.

        Arguments:
            gf (GraphFrame): the GraphFrame being queried.
            node (Node): the root node of the subgraph that is being queried.
            visited (set): a set that keeps track of what nodes have been visited in the traversal to minimize the amount of work that is repeated.
            matches (list): the list in which the final set of matches are stored.
        """
        # If the node has already been visited (or is None for some
        # reason), skip it.
        if node is None or node._hatchet_nid in visited:
            return
        # Cache the node if it's not already cached
        if node._hatchet_nid not in self.search_cache:
            self._cache_node(gf, node)
        # If the node matches the starting/root node of the query,
        # try to get all query matches in the subgraph rooted at
        # this node.
        if self.query_pattern[0][0] == "*":
            if 1 in self.search_cache[node._hatchet_nid]:
                sub_match = self._match_pattern(gf, node, 1)
                if sub_match is not None:
                    matches.extend(sub_match)
        if 0 in self.search_cache[node._hatchet_nid]:
            sub_match = self._match_pattern(gf, node, 0)
            if sub_match is not None:
                matches.extend(sub_match)
        # Note that the node is now visited.
        visited.add(node._hatchet_nid)
        # Continue the Depth First Search.
        for child in sorted(node.children, key=traversal_order):
            self._apply_impl(gf, child, visited, matches)
