# Copyright 2017-2022 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from .function import FunctionQuery
from .string import StringQuery
from .object import ObjectQuery
from .exception import BadNumberNaryQueryArgs

from abc import abstractmethod

try:
    from abc import ABC
except ImportError:
    from abc import ABCMeta

    ABC = ABCMeta("ABC", (object,), {"__slots__": ()})


class NaryQuery(ABC):
    """Abstract Base Class defining a compound query
    that acts on and merges N separate subqueries"""

    def __init__(self, *args):
        """Create a new NaryQuery object.

        Arguments:
            *args (tuple): the subqueries (high-level, low-level, or compound) to be performed.
        """
        self.subqueries = []
        if isinstance(args[0], tuple) and len(args) == 1:
            args = args[0]
        for query in args:
            if isinstance(query, list):
                self.subqueries.append(ObjectQuery(query))
            elif isinstance(query, str):
                self.subqueries.append(StringQuery(query))
            elif issubclass(type(query), (FunctionQuery, NaryQuery)):
                self.subqueries.append(query)
            else:
                raise TypeError(
                    "Subqueries for NaryQuery must be either a single query \
                    (i.e., function interface, string syntax, or object syntax) \
                    or another NaryQuery (or subclass) object."
                )

    @abstractmethod
    def _perform_nary_op(self, query_results, gf):
        """Perform the NaryQuery subclass's designated operation on the results of the subqueries.

        Arguments:
            query_results (list): the results of the subqueries.
            gf (GraphFrame): the GraphFrame on which the query is applied.

        Returns:
            (list): A list of nodes representing the result of applying the subclass-designated operation to the results of the subqueries.
        """
        pass


class AndQuery(NaryQuery):
    """Compound Query that returns the intersection of the results
    of the subqueries"""

    def __init__(self, *args):
        """Create a new AndQuery object.

        Arguments:
            *args (tuple): the subqueries (high-level, low-level, or compound) to be performed.
        """
        if sys.version_info[0] == 2:
            super(AndQuery, self).__init__(args)
        else:
            super().__init__(args)
        if len(self.subqueries) < 2:
            raise BadNumberNaryQueryArgs("AndQuery requires 2 or more subqueries")

    def _perform_nary_op(self, query_results, gf):
        """Perform an intersection operation on the results of the subqueries.

        Arguments:
            query_results (list): the results of the subqueries.
            gf (GraphFrame): the GraphFrame on which the query is applied.

        Returns:
            (list): A list of nodes representing the intersection of the results of the subqueries.
        """
        intersection_set = set(query_results[0]).intersection(*query_results[1:])
        return list(intersection_set)


"""Alias of AndQuery to signify the relationship to set Intersection"""
IntersectionQuery = AndQuery


class OrQuery(NaryQuery):
    """Compound Query that returns the union of the results
    of the subqueries"""

    def __init__(self, *args):
        """Create a new OrQuery object.

        Arguments:
            *args (tuple): the subqueries (high-level, low-level, or compound) to be performed.
        """
        if sys.version_info[0] == 2:
            super(OrQuery, self).__init__(args)
        else:
            super().__init__(args)
        if len(self.subqueries) < 2:
            raise BadNumberNaryQueryArgs("OrQuery requires 2 or more subqueries")

    def _perform_nary_op(self, query_results, gf):
        """Perform an union operation on the results of the subqueries.

        Arguments:
            query_results (list): the results of the subqueries.
            gf (GraphFrame): the GraphFrame on which the query is applied.

        Returns:
            (list): A list of nodes representing the union of the results of the subqueries.
        """
        union_set = set().union(*query_results)
        return list(union_set)


"""Alias of OrQuery to signify the relationship to set Union"""
UnionQuery = OrQuery


class XorQuery(NaryQuery):
    """Compound Query that returns the symmetric difference
    (i.e., set-based XOR) of the results of the subqueries"""

    def __init__(self, *args):
        """Create a new XorQuery object.

        Arguments:
            *args (tuple): the subqueries (high-level, low-level, or compound) to be performed.
        """
        if sys.version_info[0] == 2:
            super(XorQuery, self).__init__(args)
        else:
            super().__init__(args)
        if len(self.subqueries) < 2:
            raise BadNumberNaryQueryArgs("XorQuery requires 2 or more subqueries")

    def _perform_nary_op(self, query_results, gf):
        """Perform a symmetric difference operation on the results of the subqueries.

        Arguments:
            query_results (list): the results of the subqueries.
            gf (GraphFrame): the GraphFrame on which the query is applied.

        Returns:
            (list): A list of nodes representing the symmetric difference of the results of the subqueries.
        """
        xor_set = set()
        for res in query_results:
            xor_set = xor_set.symmetric_difference(set(res))
        return list(xor_set)


"""Alias of XorQuery to signify the relationship to set Symmetric Difference"""
SymDifferenceQuery = XorQuery


class NotQuery(NaryQuery):
    """Compound Query that returns all nodes in the GraphFrame that
    are not returned from the subquery."""

    def __init__(self, *args):
        """Create a new XorQuery object.

        Arguments:
            *args (tuple): the subquery (high-level, low-level, or compound) to be performed.
        """
        if sys.version_info[0] == 2:
            super(NotQuery, self).__init__(args)
        else:
            super().__init__(args)
        if len(self.subqueries) != 1:
            raise BadNumberNaryQueryArgs("NotQuery requires exactly 1 subquery")

    def _perform_nary_op(self, query_results, gf):
        """Collect all nodes in the graph not present in the query result.

        Arguments:
            query_results (list): the result of the subquery.
            gf (GraphFrame): the GraphFrame on which the query is applied.

        Returns:
            (list): A list of all nodes not found in the subquery.
        """
        nodes = set(gf.graph.traverse())
        query_nodes = set(query_results[0])
        return list(nodes.difference(query_nodes))


class Query(FunctionQuery):

    def __init__(self, object_or_string_query=None):
        if object_or_string_query is not None:
            if isinstance(object_or_string_query, list):
