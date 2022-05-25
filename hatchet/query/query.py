# Copyright 2017-2022 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from abc import abstractmethod

try:
    from abc import ABC
except ImportError:
    from abc import ABCMeta

    ABC = ABCMeta("ABC", (object,), {"__slots__": ()})


def Query(ABC):

    def __init__(self):
        # Initialize container for query
        self.query_pattern = []

    def match(self, wildcard_spec=".", filter_func=lambda row: True):
        """Start a query with a root node described by the arguments.

        Arguments:
            wildcard_spec (str, optional, ".", "*", or "+"): the wildcard status of the node (follows standard Regex syntax)
            filter_func (callable, optional): a callable accepting only a row from a Pandas DataFrame that is used to filter this node in the query

        Returns:
            (QueryMatcher): The instance of the class that called this function (enables fluent design).
        """
        if len(self.query_pattern) != 0:
            self.query_pattern = []
        self._add_node(wildcard_spec, filter_func)
        return self

    def rel(self, wildcard_spec=".", filter_func=lambda row: True):
        """Add another edge and node to the query.

        Arguments:
            wildcard_spec (str, optional, ".", "*", or "+"): the wildcard status of the node (follows standard Regex syntax)
            filter_func (callable, optional): a callable accepting only a row from a Pandas DataFrame that is used to filter this node in the query

        Returns:
            (QueryMatcher): The instance of the class that called this function (enables fluent design).
        """
        self._add_node(wildcard_spec, filter_func)
        return self

    def _add_node(self, wildcard_spec=".", filter_func=lambda row: True):
        """Add a node to the query.
        Arguments:
            wildcard_spec (str, optional, ".", "*", or "+"): the wildcard status of the node (follows standard Regex syntax)
            filter_func (callable, optional): a callable accepting only a row from a Pandas DataFrame that is used to filter this node in the query
        """
        assert isinstance(wildcard_spec, int) or isinstance(wildcard_spec, str)
        assert callable(filter_func)
        if isinstance(wildcard_spec, int):
            for i in range(wildcard_spec):
                self.query_pattern.append((".", filter_func))
        elif wildcard_spec == "+":
            self.query_pattern.append((".", filter_func))
            self.query_pattern.append(("*", filter_func))
        else:
            assert wildcard_spec == "." or wildcard_spec == "*"
            self.query_pattern.append((wildcard_spec, filter_func))
