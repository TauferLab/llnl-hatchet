# Copyright 2017-2021 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from .exceptions import InvalidQueryInitializer
from .query_matcher import QueryMatcher
from .mid_level import CypherQuery
from .compound import CompoundQueryMixin

class Query(CypherQuery, CompoundQueryMixin):

    def __init__(self, query=None):
        if query is None:
            CypherQuery.__mro__[1].__init__(self)
        elif isinstance(query, list):
            CypherQuery.__mro__[1].__init__(self, query)
        elif isinstance(query, str):
            CypherQuery.__init__(self, query)
        else:
            raise InvalidQueryInitializer(
                "Cannot initialize a query with input of type {}".format(
                    type(query)
                )
            )
