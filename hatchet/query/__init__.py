# Copyright 2017-2022 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from .engine import QueryEngine
from .query import Query
from .string import StringQuery
from .object import ObjectQuery
from .compound import (
    NaryQuery,
    ConjunctionQuery,
    IntersectionQuery,
    conjunction_op,
    DisjunctionQuery,
    UnionQuery,
    disjunction_op,
    ExcDisjunctionQuery,
    SymDifferenceQuery,
    exc_disjunction_op,
    ComplementQuery,
    complement_op,
)
from .deprecated import (
    AbstractQuery,
    NaryQuery,
    QueryMatcher,
    CypherQuery,
    AndQuery,
    OrQuery,
    XorQuery,
    NotQuery,
)

# Set the following bitwise operators for Query:
#  * __and__ (q1 & q2)
#  * __or__ (q1 | q2)
#  * __xor__ (q1 ^ q2)
#  * __invert__ (~q)
setattr(Query, "__and__", conjunction_op)
setattr(Query, "__or__", disjunction_op)
setattr(Query, "__xor__", exc_disjunction_op)
setattr(Query, "__invert__", complement_op)
