# Copyright 2017-2022 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from .function import Query
from .exception import (
    InvalidQueryFilter,
    InvalidQueryPath,
)

import re
import sys
import pandas as pd
from textx import metamodel_from_str
from textx.exceptions import TextXError

# Flake8 to ignore this import, it does not recognize that eval("np.nan") needs
# numpy package
import numpy as np  # noqa: F401


CYPHER_GRAMMAR = u"""
FullQuery: path_expr=MatchExpr(cond_expr=WhereExpr)?;
MatchExpr: 'MATCH' path=PathQuery;
PathQuery: '(' nodes=NodeExpr ')'('->' '(' nodes=NodeExpr ')')*;
NodeExpr: ((wcard=INT | wcard=STRING) ',' name=ID) | (wcard=INT | wcard=STRING) |  name=ID;
WhereExpr: 'WHERE' ConditionExpr;
ConditionExpr: conditions+=CompoundCond;
CompoundCond: UnaryCond | BinaryCond;
BinaryCond: AndCond | OrCond;
AndCond: 'AND' subcond=UnaryCond;
OrCond: 'OR' subcond=UnaryCond;
UnaryCond: NotCond | SingleCond;
NotCond: 'NOT' subcond=SingleCond;
SingleCond: StringCond | NumberCond | NoneCond | NotNoneCond;
NoneCond: name=ID '.' prop=STRING 'IS NONE';
NotNoneCond: name=ID '.' prop=STRING 'IS NOT NONE';
StringCond: StringEq | StringStartsWith | StringEndsWith | StringContains | StringMatch;
StringEq: name=ID '.' prop=STRING '=' val=STRING;
StringStartsWith: name=ID '.' prop=STRING 'STARTS WITH' val=STRING;
StringEndsWith: name=ID '.' prop=STRING 'ENDS WITH' val=STRING;
StringContains: name=ID '.' prop=STRING 'CONTAINS' val=STRING;
StringMatch: name=ID '.' prop=STRING '=~' val=STRING;
NumberCond: NumEq | NumLt | NumGt | NumLte | NumGte | NumNan | NumNotNan | NumInf | NumNotInf;
NumEq: name=ID '.' prop=STRING '=' val=NUMBER;
NumLt: name=ID '.' prop=STRING '<' val=NUMBER;
NumGt: name=ID '.' prop=STRING '>' val=NUMBER;
NumLte: name=ID '.' prop=STRING '<=' val=NUMBER;
NumGte: name=ID '.' prop=STRING '>=' val=NUMBER;
NumNan: name=ID '.' prop=STRING 'IS NAN';
NumNotNan: name=ID '.' prop=STRING 'IS NOT NAN';
NumInf: name=ID '.' prop=STRING 'IS INF';
NumNotInf: name=ID '.' prop=STRING 'IS NOT INF';
"""

cypher_query_mm = metamodel_from_str(CYPHER_GRAMMAR)


def cname(obj):
    return obj.__class__.__name__


def filter_check_types(type_check, df_row, filt_lambda):
    try:
        if type_check == "" or eval(type_check):
            return filt_lambda(df_row)
        else:
            raise InvalidQueryFilter("Type mismatch in filter")
    except KeyError:
        return False


class StringQuery(Query):
    def __init__(self, cypher_query):
        if sys.version_info[0] == 2:
            super(StringQuery, self).__init__()
        else:
            super().__init__()
        model = None
        try:
            model = cypher_query_mm.model_from_str(cypher_query)
        except TextXError as e:
            # TODO Change to a "raise-from" expression when Python 2.7 support is dropped
            raise InvalidQueryPath(
                'Invalid String Query Detected. Parser Error Message: {}'.format(
                    e.message
                )
            )
        self.wcards = []
        self.wcard_pos = {}
        self._parse_path(model.path_expr)
        self.filters = [[] for _ in self.wcards]
        self._parse_conditions(model.cond_expr)
        self.lambda_filters = [None for _ in self.wcards]
        self._build_lambdas()
        self._build_query()

    def _build_query(self):
        for i in range(0, len(self.wcards)):
            wcard = self.wcards[i][0]
            # TODO Remove this when Python 2.7 support is dropped.
            if sys.version_info[0] == 2 and not isinstance(wcard, Real):
                wcard = wcard.encode("ascii", "ignore")
            filt_str = self.lambda_filters[i]
            if filt_str is None:
                if i == 0:
                    self.match(wildcard_spec=wcard)
                else:
                    self.rel(wildcard_spec=wcard)
            else:
                if i == 0:
                    self.match(wildcard_spec=wcard, filter_func=eval(filt_str))
                else:
                    self.rel(wildcard_spec=wcard, filter_func=eval(filt_str))

    def _build_lambdas(self):
        for i in range(0, len(self.wcards)):
            n = self.wcards[i]
            if n[1] != "":
                bool_expr = ""
                type_check = ""
                for j, cond in enumerate(self.filters[i]):
                    if cond[0] is not None:
                        bool_expr += " {}".format(cond[0])
                    bool_expr += " {}".format(cond[1])
                    if cond[2] is not None:
                        if j == 0:
                            type_check += " {}".format(cond[2])
                        else:
                            type_check += " and {}".format(cond[2])
                bool_expr = "lambda df_row: {}".format(bool_expr)
                bool_expr = (
                    'lambda df_row: filter_check_types("{}", df_row, {})'.format(
                        type_check, bool_expr
                    )
                )
                self.lambda_filters[i] = bool_expr

    def _parse_path(self, path_obj):
        nodes = path_obj.path.nodes
        idx = len(self.wcards)
        for n in nodes:
            new_node = [n.wcard, n.name]
            if n.wcard is None or n.wcard == "" or n.wcard == 0:
                new_node[0] = "."
            self.wcards.append(new_node)
            if n.name != "":
                self.wcard_pos[n.name] = idx
            idx += 1

    def _parse_conditions(self, cond_expr):
        conditions = cond_expr.conditions
        for cond in conditions:
            converted_condition = None
            if self._is_unary_cond(cond):
                converted_condition = self._parse_unary_cond(cond)
            elif self._is_binary_cond(cond):
                converted_condition = self._parse_binary_cond(cond)
            else:
                raise RuntimeError("Bad Condition")
            self.filters[self.wcard_pos[converted_condition[1]]].append(
                [converted_condition[0], converted_condition[2], converted_condition[3]]
            )
        for i in range(0, len(self.filters)):
            if len(self.filters[i]) > 0:
                if self.filters[i][0][0] != "not":
                    self.filters[i][0][0] = None

    def _is_unary_cond(self, obj):
        if (
            cname(obj) == "NotCond"
            or self._is_str_cond(obj)
            or self._is_num_cond(obj)
            or cname(obj) in ["NoneCond", "NotNoneCond"]
        ):
            return True
        return False

    def _is_binary_cond(self, obj):
        if cname(obj) in ["AndCond", "OrCond"]:
            return True
        return False

    def _parse_binary_cond(self, obj):
        if cname(obj) == "AndCond":
            return self._parse_and_cond(obj)
        if cname(obj) == "OrCond":
            return self._parse_or_cond(obj)
        raise RuntimeError("Bad Binary Condition")

    def _parse_or_cond(self, obj):
        converted_subcond = self._parse_unary_cond(obj.subcond)
        converted_subcond[0] = "or"
        return converted_subcond

    def _parse_and_cond(self, obj):
        converted_subcond = self._parse_unary_cond(obj.subcond)
        converted_subcond[0] = "and"
        return converted_subcond

    def _parse_unary_cond(self, obj):
        if cname(obj) == "NotCond":
            return self._parse_not_cond(obj)
        return self._parse_single_cond(obj)

    def _parse_not_cond(self, obj):
        converted_subcond = self._parse_single_cond(obj.subcond)
        converted_subcond[2] = "not {}".format(converted_subcond[2])
        return converted_subcond

    def _parse_single_cond(self, obj):
        if self._is_str_cond(obj):
            return self._parse_str(obj)
        if self._is_num_cond(obj):
            return self._parse_num(obj)
        if cname(obj) == "NoneCond":
            return self._parse_none(obj)
        if cname(obj) == "NotNoneCond":
            return self._parse_not_none(obj)
        raise RuntimeError("Bad Single Condition")

    def _parse_none(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth is None",
                None,
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid is None",
                None,
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] is None'.format(obj.prop),
            None,
        ]

    def _parse_not_none(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth is not None",
                None,
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid is not None",
                None,
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] is not None'.format(obj.prop),
            None,
        ]

    def _is_str_cond(self, obj):
        if cname(obj) in [
            "StringEq",
            "StringStartsWith",
            "StringEndsWith",
            "StringContains",
            "StringMatch",
        ]:
            return True
        return False

    def _is_num_cond(self, obj):
        if cname(obj) in [
            "NumEq",
            "NumLt",
            "NumGt",
            "NumLte",
            "NumGte",
            "NumNan",
            "NumNotNan",
            "NumInf",
            "NumNotInf",
        ]:
            return True
        return False

    def _parse_str(self, obj):
        if cname(obj) == "StringEq":
            return self._parse_str_eq(obj)
        if cname(obj) == "StringStartsWith":
            return self._parse_str_starts_with(obj)
        if cname(obj) == "StringEndsWith":
            return self._parse_str_ends_with(obj)
        if cname(obj) == "StringContains":
            return self._parse_str_contains(obj)
        if cname(obj) == "StringMatch":
            return self._parse_str_match(obj)
        raise RuntimeError("Bad String Op Class")

    def _parse_str_eq(self, obj):
        return [
            None,
            obj.name,
            'df_row["{}"] == "{}"'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], str)".format(obj.prop),
        ]

    def _parse_str_starts_with(self, obj):
        return [
            None,
            obj.name,
            'df_row["{}"].startswith("{}")'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], str)".format(obj.prop),
        ]

    def _parse_str_ends_with(self, obj):
        return [
            None,
            obj.name,
            'df_row["{}"].endswith("{}")'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], str)".format(obj.prop),
        ]

    def _parse_str_contains(self, obj):
        return [
            None,
            obj.name,
            '"{}" in df_row["{}"]'.format(obj.val, obj.prop),
            "isinstance(df_row['{}'], str)".format(obj.prop),
        ]

    def _parse_str_match(self, obj):
        return [
            None,
            obj.name,
            're.match("{}", df_row["{}"]) is not None'.format(obj.val, obj.prop),
            "isinstance(df_row['{}'], str)".format(obj.prop),
        ]

    def _parse_num(self, obj):
        if cname(obj) == "NumEq":
            return self._parse_num_eq(obj)
        if cname(obj) == "NumLt":
            return self._parse_num_lt(obj)
        if cname(obj) == "NumGt":
            return self._parse_num_gt(obj)
        if cname(obj) == "NumLte":
            return self._parse_num_lte(obj)
        if cname(obj) == "NumGte":
            return self._parse_num_gte(obj)
        if cname(obj) == "NumNan":
            return self._parse_num_nan(obj)
        if cname(obj) == "NumNotNan":
            return self._parse_num_not_nan(obj)
        if cname(obj) == "NumInf":
            return self._parse_num_inf(obj)
        if cname(obj) == "NumNotInf":
            return self._parse_num_not_inf(obj)
        raise RuntimeError("Bad Number Op Class")

    def _parse_num_eq(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth == {}".format(obj.val),
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid == {}".format(obj.val),
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] == {}'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_lt(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth < {}".format(obj.val),
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid < {}".format(obj.val),
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] < {}'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_gt(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth > {}".format(obj.val),
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid > {}".format(obj.val),
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] > {}'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_lte(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth <= {}".format(obj.val),
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid <= {}".format(obj.val),
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] <= {}'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_gte(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "df_row.name._depth >= {}".format(obj.val),
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "df_row.name._hatchet_nid >= {}".format(obj.val),
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'df_row["{}"] >= {}'.format(obj.prop, obj.val),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_nan(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "pd.isna(df_row.name._depth)",
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "pd.isna(df_row.name._hatchet_nid)",
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'pd.isna(df_row["{}"])'.format(obj.prop),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_not_nan(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "not pd.isna(df_row.name._depth)",
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "not pd.isna(df_row.name._hatchet_nid)",
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'not pd.isna(df_row["{}"])'.format(obj.prop),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_inf(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "np.isinf(df_row.name._depth)",
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "np.isinf(df_row.name._hatchet_nid)",
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'np.isinf(df_row["{}"])'.format(obj.prop),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]

    def _parse_num_not_inf(self, obj):
        if obj.prop == "depth":
            return [
                None,
                obj.name,
                "not np.isinf(df_row.name._depth)",
                "isinstance(df_row.name._depth, Real)",
            ]
        if obj.prop == "node_id":
            return [
                None,
                obj.name,
                "not np.isinf(df_row.name._hatchet_nid)",
                "isinstance(df_row.name._hatchet_nid, Real)",
            ]
        return [
            None,
            obj.name,
            'not np.isinf(df_row["{}"])'.format(obj.prop),
            "isinstance(df_row['{}'], Real)".format(obj.prop),
        ]
