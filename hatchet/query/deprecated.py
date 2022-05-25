# Copyright 2017-2022 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from .engine import QueryEngine
from .query import Query
from .string import StringQuery
from .object import ObjectQuery
from .compound import (
    CompoundQuery,
    ConjunctionQuery,
    DisjunctionQuery,
    ExcDisjunctionQuery,
    ComplementQuery,
)

from abc import abstractmethod

try:
    from abc import ABC
except ImportError:
    from abc import ABCMeta

    ABC = ABCMeta("ABC", (object,), {"__slots__": ()})


deprecated_engine_singleton = QueryEngine()


class AbstractQuery(ABC):
    """Abstract Base Class defining a Hatchet Query"""

    @abstractmethod
    def apply(self, gf):
        """Apply the query to a GraphFrame.

        Arguments:
            gf (GraphFrame): the GraphFrame on which to apply the query.

        Returns:
            (list): A list representing the set of nodes from paths that match this query.
        """
        pass


class NaryQuery(AbstractQuery, CompoundQuery):

    def __init__(self, *args):
        ComplementQuery.__init__(self, *args)

    def apply(self, gf):
        """Apply the NaryQuery to a GraphFrame.

        Arguments:
            gf (GraphFrame): the GraphFrame on which to apply the query.

        Returns:
            (list): A list of nodes representing the result of applying the subclass-designated operation to the results of the subqueries.
        """
        results = []
        for query in self.subqueries:
            results.append(deprecated_engine_singleton.apply(gf, query))
        return self._perform_nary_op(results, gf)


class QueryMatcher(AbstractQuery, ObjectQuery, Query):

    def __init__(self, query=None):
        if query is None:
            Query.__init__(self)
        else:
            ObjectQuery.__init__(self, query)

    def apply(self, gf):
        return deprecated_engine_singleton.apply(gf, self.query)


class CypherQuery(QueryMatcher, StringQuery):

    def __init__(self, cypher_query):
        StringQuery.__init__(self, cypher_query)


class AndQuery(NaryQuery, ConjunctionQuery):

    def __init__(self, *args):
        ConjunctionQuery.__init__(self, *args)


class OrQuery(NaryQuery, DisjunctionQuery):

    def __init__(self, *args):
        DisjunctionQuery.__init__(self, *args)


class XorQuery(NaryQuery, ExcDisjunctionQuery):

    def __init__(self, *args):
        ExcDisjunctionQuery.__init__(self, *args)


class NotQuery(NaryQuery, ComplementQuery):

    def __init__(self, *args):
        ComplementQuery.__init__(self, *args)
