# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from numbers import Real
from pandas.api.types import (
    is_numeric_dtype,
    is_string_dtype,
)
import sys

from .errors import InvalidQueryPath, InvalidQueryFilter
from .query import Query


def _process_predicate(attr_filter):
    """Converts high-level API attribute filter to a lambda"""
    compops = ("<", ">", "==", ">=", "<=", "<>", "!=")  # ,
    pandas_exprs = []
    for metric, cond in attr_filter:
        if metric == "depth":
            if isinstance(cond, str) and cond.lower().startswith(compops):
                pandas_exprs.append(
                    "df.index.get_level_values(0).apply(lambda n: n._depth {})".format(cond)
                )
            elif isinstance(cond, Real):
                pandas_exprs.append(
                    "df.index.get_level_values(0).apply(lambda n: n._depth == {})".format(cond)
                )
            raise InvalidQueryFilter(
                "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                    metric
                )
            )
        if metric == "node_id":
            if isinstance(cond, str) and cond.lower().startswith(compops):
                pandas_exprs.append(
                    "df.index.get_level_values(0).apply(lambda n: n._hatchet_nid {})".format(cond)
                )
            elif isinstance(cond, Real):
                pandas_exprs.append(
                    "df.get_level_values(0).apply(lambda n: n._hatchet_nid == {})".format(cond)
                )
            raise InvalidQueryFilter(
                "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                    metric
                )
            )
    
    def apply_predicate(df):

        for metric, cond in attr_filter:
            if metric not in df.columns:
                return False
            if is_string_dtype(df[metric]):
                pandas_exprs.append(
                    "df[{}].str.match(r\"{}\\Z\")".format(metric, cond)
                )
            if is_numeric_dtype(df[metric]):
                if isinstance(cond, str) and cond.lower().startswith(compops):
                    pandas_exprs.append(
                        "(df[{}] {})".format(metric, cond)
                    )
                elif isinstance(cond, Real):
                    pandas_exprs.append(
                        "(df[{}] == {})".format(metric, cond)
                    )
                raise InvalidQueryFilter(
                    "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                        metric
                    )
                )
            raise InvalidQueryFilter(
                "Filter must be one of the following:\n  * A regex string for a String attribute\n  * A string starting with a comparison operator for a Numeric attribute\n  * A number for a Numeric attribute\n"
            )
        
        if len(pandas_exprs) == 0:
            raise InvalidQueryFilter(
                "Constructed predicate contains 0 sub-expressions"
            )
        full_pd_expr = " & ".join(pandas_exprs)
        return eval(full_pd_expr)

    return apply_predicate if attr_filter != {} else lambda row: True


class ObjectQuery(Query):

    """Class for representing and parsing queries using the Object-based dialect."""

    def __init__(self, query):
        """Builds a new ObjectQuery from an instance of the Object-based dialect syntax.

        Arguments:
            query (list): the Object-based dialect query to parse and store
        """
        if sys.version_info[0] == 2:
            super(ObjectQuery, self).__init__()
        else:
            super().__init__()
        assert isinstance(query, list)
        for qnode in query:
            if isinstance(qnode, dict):
                self._add_node(predicate=_process_predicate(qnode))
            elif isinstance(qnode, str) or isinstance(qnode, int):
                self._add_node(quantifer=qnode)
            elif isinstance(qnode, tuple):
                assert isinstance(qnode[1], dict)
                if isinstance(qnode[0], str) or isinstance(qnode[0], int):
                    self._add_node(
                        qnode[0], _process_predicate(qnode[1])
                    )
                else:
                    raise InvalidQueryPath(
                        "The first value of a tuple entry in a path must be either a string or integer."
                    )
            else:
                raise InvalidQueryPath(
                    "A query path must be a list containing String, Integer, Dict, or Tuple elements"
                )
