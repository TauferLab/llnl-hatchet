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

from hatchet.query.errors import InvalidQueryPath, InvalidQueryFilter
from hatchet.query.v2.query import Query


def _process_predicate(attr_filter):
    """Converts high-level API attribute filter to a lambda"""
    compops = ("<", ">", "==", ">=", "<=", "<>", "!=")  # ,
    
    def apply_predicate(df):
        pandas_exprs = []
        for metric, cond in attr_filter.items():
            if metric == "depth":
                if isinstance(cond, str) and cond.lower().startswith(compops):
                    pandas_exprs.append(
                        "df.index.get_level_values(0).to_series().apply(lambda n: n._depth {})".format(cond)
                    )
                elif isinstance(cond, Real):
                    if cond == -1:
                        pandas_exprs.append(
                            "df.index.get_level_values(0).to_series().apply(lambda n: len(n.children) == 0)"
                        )
                    else:
                        pandas_exprs.append(
                            "df.index.get_level_values(0).to_series().apply(lambda n: n._depth == {})".format(cond)
                        )
                elif isinstance(cond, list) or isinstance(cond, tuple):
                    for subcond in cond:
                        if subcond == -1:
                            pandas_exprs.append(
                                "df.index.get_level_values(0).to_series().apply(lambda n: len(n.children) == 0)"
                            )
                        elif isinstance(subcond, str) and subcond.lower().startswith(compops):
                            pandas_exprs.append(
                                "df.index.get_level_values(0).to_series().apply(lambda n: n._depth {})".format(cond)
                            )
                        elif isinstance(subcond, Real):
                            pandas_exprs.append(
                                "df.index.get_level_values(0).to_series().apply(lambda n: n._depth == {})".format(subcond)
                            )
                        else:
                            raise InvalidQueryFilter(
                                "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                                    metric
                                )
                            )
                else:
                    raise InvalidQueryFilter(
                        "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                            metric
                        )
                    )
            elif metric == "node_id":
                if isinstance(cond, str) and cond.lower().startswith(compops):
                    pandas_exprs.append(
                        "df.index.get_level_values(0).to_series().apply(lambda n: n._hatchet_nid {})".format(cond)
                    )
                elif isinstance(cond, Real):
                    pandas_exprs.append(
                        "df.index.get_level_values(0).to_series().apply(lambda n: n._hatchet_nid == {})".format(cond)
                    )
                elif isinstance(cond, list) or isinstance(cond, tuple):
                    for subcond in cond:
                        if isinstance(subcond, str) and subcond.lower().startswith(compops):
                            pandas_exprs.append(
                                "df.index.get_level_values(0).to_series().apply(lambda n: n._hatchet_nid {})".format(subcond)
                            )
                        elif isinstance(subcond, Real):
                            pandas_exprs.append(
                                "df.index.get_level_values(0).to_series().apply(lambda n: n._hatchet_nid == {})".format(subcond)
                            )
                        else:
                            raise InvalidQueryFilter(
                                "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                                    metric
                                )
                            )
                else:
                    raise InvalidQueryFilter(
                        "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                            metric
                        )
                    )
            elif metric not in df.columns:
                return False
            elif is_numeric_dtype(df[metric]):
                if isinstance(cond, str) and cond.lower().startswith(compops):
                    pandas_exprs.append(
                        "(df[\"{}\"] {})".format(metric, cond)
                    )
                elif isinstance(cond, Real):
                    pandas_exprs.append(
                        "(df[\"{}\"] == {})".format(metric, cond)
                    )
                elif isinstance(cond, list) or isinstance(cond, tuple):
                    for subcond in cond:
                        if isinstance(subcond, str) and subcond.lower().startswith(compops):
                            pandas_exprs.append(
                                "(df[\"{}\"] {})".format(metric, subcond)
                            )
                        elif isinstance(subcond, Real):
                            pandas_exprs.append(
                                "(df[\"{}\"] == {})".format(metric, subcond)
                            )
                        else:
                            raise InvalidQueryFilter(
                                "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                                    metric
                                )
                            )
                else:
                    raise InvalidQueryFilter(
                        "Attribute {} has a numeric type. Valid filters for this attribute are a string starting with a comparison operator or a real number.".format(
                            metric
                        )
                    )
            elif is_string_dtype(df[metric]) and isinstance(cond, str):
                pandas_exprs.append(
                    "df[\"{}\"].str.match(r\"{}\\Z\")".format(metric, cond)
                )
            else:
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
                if len(qnode) > 0:
                    self._add_node(predicate=_process_predicate(qnode))
                else:
                    self._add_node()
            elif isinstance(qnode, str) or isinstance(qnode, int):
                self._add_node(quantifer=qnode)
            elif isinstance(qnode, tuple):
                assert isinstance(qnode[1], dict)
                if isinstance(qnode[0], str) or isinstance(qnode[0], int):
                    if len(qnode[1]) > 0:
                        self._add_node(
                            qnode[0], _process_predicate(qnode[1])
                        )
                    else:
                        self._add_node(qnode[0])
                else:
                    raise InvalidQueryPath(
                        "The first value of a tuple entry in a path must be either a string or integer."
                    )
            else:
                raise InvalidQueryPath(
                    "A query path must be a list containing String, Integer, Dict, or Tuple elements"
                )
