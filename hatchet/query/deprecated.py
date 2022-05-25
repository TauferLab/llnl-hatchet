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

import warnings
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
        warnings.warn(
            """You are using the old Query Language class QueryMatcher!
            This class is deprecated!
            Please switch to either the Query or ObjectQuery class!
            """,
            DeprecationWarning
        )
        if query is None:
            Query.__init__(self)
        else:
            ObjectQuery.__init__(self, query)

    def apply(self, gf):
        return deprecated_engine_singleton.apply(gf, self.query)


class CypherQuery(QueryMatcher, StringQuery):

    def __init__(self, cypher_query):
        warnings.warn(
            """You are using the old Query Language class CypherQuery!
            This class is deprecated!
            Please switch to the StringQuery class!
            """,
            DeprecationWarning
        )
        StringQuery.__init__(self, cypher_query)


class AndQuery(NaryQuery, ConjunctionQuery):

    def __init__(self, *args):
        warnings.warn(
            """AndQuery is deprecated in its current form!
            In the future, AndQuery will still be available as an alias to ConjunctionQuery,
            but AndQuery's "apply" method will be removed!
            """,
            DeprecationWarning
        )
        ConjunctionQuery.__init__(self, *args)


class OrQuery(NaryQuery, DisjunctionQuery):

    def __init__(self, *args):
        warnings.warn(
            """OrQuery is deprecated in its current form!
            In the future, OrQuery will still be available as an alias to DisjunctionQuery,
            but OrQuery's "apply" method will be removed!
            """,
            DeprecationWarning
        )
        DisjunctionQuery.__init__(self, *args)


class XorQuery(NaryQuery, ExcDisjunctionQuery):

    def __init__(self, *args):
        warnings.warn(
            """XorQuery is deprecated in its current form!
            In the future, XorQuery will still be available as an alias to ExcDisjunctionQuery,
            but XorQuery's "apply" method will be removed!
            """,
            DeprecationWarning
        )
        ExcDisjunctionQuery.__init__(self, *args)


class NotQuery(NaryQuery, ComplementQuery):

    def __init__(self, *args):
        warnings.warn(
            """NotQuery is deprecated in its current form!
            In the future, NotQuery will still be available as an alias to ComplementQuery,
            but NotQuery's "apply" method will be removed!
            """,
            DeprecationWarning
        )
        ComplementQuery.__init__(self, *args)
