from hatchet.query.v2.query import Query
from hatchet.query.v2.string_dialect import (
    StringQuery,
    parse_string_dialect
)
from hatchet.query.v2.object_dialect import ObjectQuery
from hatchet.query.v2.engine import QueryEngine

from hatchet.query.errors import (
    InvalidQueryFilter,
    InvalidQueryPath,
    RedundantQueryFilterWarning,
    BadNumberNaryQueryArgs,
    MultiIndexModeMismatch,
)

from hatchet.query.v2.compound import (
    CompoundQuery,
    ConjunctionQuery,
    DisjunctionQuery,
    ExclusiveDisjunctionQuery,
    NegationQuery,
)


def combine_via_conjunction(query0, query1):
    return ConjunctionQuery(query0, query1)


def combine_via_disjunction(query0, query1):
    return DisjunctionQuery(query0, query1)


def combine_via_exclusive_disjunction(query0, query1):
    return ExclusiveDisjunctionQuery(query0, query1)


def negate_query(query):
    return NegationQuery(query)


Query.__and__ = combine_via_conjunction
Query.__or__ = combine_via_disjunction
Query.__xor__ = combine_via_exclusive_disjunction
Query.__not__ = negate_query


CompoundQuery.__and__ = combine_via_conjunction
CompoundQuery.__or__ = combine_via_disjunction
CompoundQuery.__xor__ = combine_via_exclusive_disjunction
CompoundQuery.__not__ = negate_query


def is_hatchet_query(query_obj):
    return (
        issubclass(type(query_obj), Query)
        or issubclass(type(query_obj), CompoundQuery)
    )