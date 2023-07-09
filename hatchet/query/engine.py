# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd

from .errors import MultiIndexModeMismatch
from ..node import traversal_order
from .query import Query
from .compound import CompoundQuery
from .object_dialect import ObjectQuery
from .string_dialect import parse_string_dialect


class QueryEngine:

    """Class for applying queries to GraphFrames."""
    
    _available_modes = (
        "off",
        "any",
        "all",
    )

    def __init__(self):
        """Creates the QueryEngine."""
        self.candidates = []
        self.path_cache = {}

    def apply(self, query, graph, dframe, multi_index_mode="off"):
        """Apply the query to a GraphFrame.

        Arguments:
            query (Query or CompoundQuery): the query being applied
            graph (Graph): the Graph to which the query is being applied
            dframe (pandas.DataFrame): the DataFrame associated with the graph

        Returns:
            (list): A list representing the set of nodes from paths that match the query
        """
        if issubclass(type(query), Query):
            self._reset()
            self._init(query, dframe, multi_index_mode)
            return self._apply_impl(graph, query)
        elif issubclass(type(query), CompoundQuery):
            results = []
            for subq in query.subqueries:
                subq_obj = subq
                if isinstance(subq, list):
                    subq_obj = ObjectQuery(subq)
                elif isinstance(subq, str):
                    subq_obj = parse_string_dialect(subq)
                results.append(self.apply(subq_obj, graph, dframe))
            return query._apply_op_to_results(results, graph)
        else:
            raise TypeError("Invalid query data type ({})".format(str(type(query))))

    def _reset(self):
        """Resets the cache in the QueryEngine."""
        self.candidates = []
        self.path_cache = {}
        
    def _init(self, query, dframe, multi_index_mode):
        if multi_index_mode not in self._available_modes:
            raise ValueError("Invalid multi-index mode")
        predicate_outputs = []
        for _, pred in query.query_pattern:
            predicate_outputs.append(pred(dframe))
        predicate_vals = pd.DataFrame(
            {i: po for i, po in enumerate(predicate_outputs)}
        )
        if isinstance(predicate_vals.index, pd.MultiIndex):
            if multi_index_mode == "off":
                raise MultiIndexModeMismatch("Cannot use 'off' mode with multi-indexed data")
            level_names = list(predicate_vals.index.names)
            level_names.remove("node")
            predicate_vals.reset_index(inplace=True)
            predicate_vals.drop(columns=level_names, inplace=True)
            predicate_vals = predicate_vals.groupby(["node"]).aggregate(
                multi_index_mode
            )
        for i in range(len(query.query_pattern)):
            self.candidates.append(
                predicate_vals.index[predicate_vals[i]].tolist()
            )
            
    def _apply_impl(self, graph, query):
        matches = set()
        for root in sorted(graph.roots, key=traversal_order):
            root_matches = self._find_matches_from_node(
                root,
                query,
                0
            )
            if root_matches is None:
                continue
            for p in root_matches:
                for node in p:
                    matches.add(n)
        return list(matches)
            
    def _find_matches_from_node(self, curr_node, query, query_idx):
        # If we've already visited this graph node while processing this
        # query node, just return the cached value
        if curr_node in self.path_cache[(curr_node, query_idx)]:
            return self.path_cache[(curr_node, query_idx)]
        next_query_idx = None
        # If the query node has a match 1 quantifier:
        if query.query_pattern[query_idx][0] == ".":
            # Return an invalid result if the graph node doesn't
            # match the query node
            if curr_node not in self.candidates[query_idx]:
                self.path_cache[(curr_node, query_idx)] = None
                return None
            # Otherwise, set next_query_idx to point to the next
            # query node
            next_query_idx = query_idx + 1
        # If the query node has a match 0 or more quantifier:
        if query.query_pattern[query_idx][0] == "*":
            # Check if the graph node matches the next query node
            # because that's how "*" is broken
            # If the graph node does match the next query node,
            # recursively call this function to process the next
            # query node
            if curr_node in self.candidates[query_idx + 1]:
                return self._find_matches_from_node(
                    curr_node,
                    query,
                    query_idx + 1
                )
            # If the graph node does not match the current query node,
            # the state of the current path depends on whether the query
            # node is the last node of the query. If it is the last node,
            # return an "end-of-path" result. Otherwise, return an invalid result
            elif curr_node not in self.candidates[query_idx]:
                if query_idx < len(query) - 1:
                    self.path_cache[(curr_node, query_idx)] = None
                    return None
                else:
                    self.path_cache[(curr_node, query_idx)] = [[]]
                    return [()]
            # If neither of the above conditions are hit, we will continue
            # processing the current query node
            next_query_idx = query_idx
        child_paths = set()
        # For each child of the graph node
        for child in curr_node.children:
            # Recursively call this function to process the next graph node
            subpaths = self._find_matches_from_node(
                child,
                query,
                next_query_idx
            )
            # If the next graph node produces no valid sub-matches,
            # ignore its results
            if child_paths is None:
                continue
            # If the next graph node produces valid sub-matches,
            # add those matches to child_paths
            # Cast the sub-matches to tuples so that they can be added to
            # the set
            for p in subpaths:
                child_paths.add(tuple(p))
        # If we ended up with no valid sub-matches,
        # the current path is invalid, so we return None
        if len(child_paths) == 0:
            self.path_cache[(curr_node, query_idx)] = None
            return None
        # Prepend the current node to all the sub-matches
        # and return those sub-matches
        valid_child_paths = [
            (curr_node, *p)
            for p in child_paths
        ]
        self.path_cache[(curr_node, query_idx)] = valid_child_paths
        return valid_child_paths