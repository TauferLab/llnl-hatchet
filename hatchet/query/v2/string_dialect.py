# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys
import pandas as pd  # noqa: F401
from pandas.api.types import is_numeric_dtype, is_string_dtype  # noqa: F401
import numpy as np  # noqa: F401
from textx import metamodel_from_str
from textx.exceptions import TextXError

from hatchet.query.errors import InvalidQueryPath, InvalidQueryFilter, RedundantQueryFilterWarning
from hatchet.query.v2.query import Query


# PEG grammar for the String-based dialect
STRING_DIALECT_GRAMMAR = u"""
FullQuery: match_expr=MatchExpr where_expr=WhereExpr;
MatchExpr: 'MATCH' path=PathQuery;
PathQuery: '(' nodes=NodeExpr ')'('->' '(' nodes=NodeExpr ')')*;
NodeExpr: (quant=QuantExpr ',' name=ID) | (quant=QuantExpr) |  name=ID;
QuantExpr: quantifier=INT | quantifier=STRING;
WhereExpr: 'WHERE' PredicateExpr;
PredicateExpr: predicates+=CompoundPred;
CompoundPred: UnaryPred | BinaryPred;
BinaryPred: AndPred | OrPred;
AndPred: 'AND' subpred=UnaryPred;
OrPred: 'OR' subpred=UnaryPred;
UnaryPred: NotPred | SinglePred;
NotPred: 'NOT' subpred=SinglePred;
SinglePred: StringPred | NumberPred | NonePred | NotNonePred | LeafPred | NotLeafPred;
NonePred: name=ID '.' metric=STRING 'IS NONE';
NotNonePred: name=ID '.' metric=STRING 'IS NOT NONE';
LeafPred: name=ID 'IS LEAF';
NotLeafPred: name=ID 'IS NOT LEAF';
StringPred: StringEq | StringStartsWith | StringEndsWith | StringContains | StringMatch;
StringEq: name=ID '.' metric=STRING '=' val=STRING;
StringStartsWith: name=ID '.' metric=STRING 'STARTS WITH' val=STRING;
StringEndsWith: name=ID '.' metric=STRING 'ENDS WITH' val=STRING;
StringContains: name=ID '.' metric=STRING 'CONTAINS' val=STRING;
StringMatch: name=ID '.' metric=STRING '=~' val=STRING;
NumberPred: NumEq | NumLt | NumGt | NumLte | NumGte | NumNan | NumNotNan | NumInf | NumNotInf;
NumEq: name=ID '.' metric=STRING '=' val=NUMBER;
NumLt: name=ID '.' metric=STRING '<' val=NUMBER;
NumGt: name=ID '.' metric=STRING '>' val=NUMBER;
NumLte: name=ID '.' metric=STRING '<=' val=NUMBER;
NumGte: name=ID '.' metric=STRING '>=' val=NUMBER;
NumNan: name=ID '.' metric=STRING 'IS NAN';
NumNotNan: name=ID '.' metric=STRING 'IS NOT NAN';
NumInf: name=ID '.' metric=STRING 'IS INF';
NumNotInf: name=ID '.' metric=STRING 'IS NOT INF';
"""

cypher_query_mm = metamodel_from_str(STRING_DIALECT_GRAMMAR, auto_init_attributes=False)


def cname(obj):
    """Utility function to get the name of the rule represented by the input"""
    return obj.__class__.__name__


class StringQuery(Query):
    
    _predicate_classes = {
        # Unary predicates not associated with a particular data type
        "unary": (
            "NotPred",
            "NonePred",
            "NotNonePred",
            "LeafPred",
            "NotLeafPred",
        ),
        # Unary predicates associated with string data
        "string": (
            "StringEq",
            "StringStartsWith",
            "StringEndsWith",
            "StringContains",
            "StringMatch",
        ),
        # Unary predicates associated with numeric data
        "num": (
            "NumEq",
            "NumLt",
            "NumGt",
            "NumLte",
            "NumGte",
            "NumNan",
            "NumNotNan",
            "NumInf",
            "NumNotInf",
        ),
        # Binary predicates
        "binary": (
            "AndPred",
            "OrPred",
        )
    }

    def __init__(self, string_dialect_query):
        if sys.version_info[0] == 2:
            super(StringQuery, self).__init__()
        else:
            super().__init__()
        model = None
        try:
            model = cypher_query_mm.model_from_str(string_dialect_query)
        except TextXError as e:
            raise InvalidQueryPath(
                "Invalid string-dialect query detected. Parser error message: {}".format(
                    e.message
                )
            )
        self.quantifiers = []
        self.predicates = {}
        self._parse_match(model.match_expr)
        self._parse_where(model.where_expr)
        self._build_query()
        
    def _build_query(self):
        
        def _predicate_builder(type_checks, pred_expr):

            def _predicate(df):
                full_check_bool = True
                for check in type_checks.values():
                    full_check_bool = full_check_bool and eval(check)
                if not full_check_bool:
                    return False
                return eval(" ".join(pred_expr))
            
        for quantifier, id in self.quantifiers:
            if id is None:
                self._add_node(
                    quantifer=quantifier,
                    predicate=_predicate_builder({}, [
                        "pd.Series([True]", "*", "len(df),", "dtype=bool)"
                    ])
                )
            else:
                checks = self.predicates[id]["metric_checks"]
                pred = self.predicates[id]["pred"]
                self._add_node(
                    quantifer=quantifier,
                    predicate=_predicate_builder(checks, pred)
                )

    def _parse_match(self, match_expr):
        nodes = match_expr.path.nodes
        for n in nodes:
            new_node = (n.quant, n.name)
            if (n.quant is None
                    or n.quant.quantifer == ""
                    or n.quant.quantifer == 0):
                new_node[0] = "."
            self.quantifiers.append(new_node)

    def _parse_where(self, where_expr):
        predicates = where_expr.predicates
        for pred in predicates:
            parsed_pred = None
            if self._is_unary_pred(pred):
                # Parse the unary predicate
                # Note: all underlying parsing functions return a 3-tuple:
                #   0) the ID for the parsed predicate (i.e., from MATCH)
                #   1) a dict with metric names as keys and type checking code as vals
                #   2) a list containing the new contents to add to the predicate
                parsed_pred = self._parse_unary_pred(pred)
            elif self._is_binary_pred(pred):
                # Parse the binary predicate
                # Note: all underlying parsing functions return a 3-tuple:
                #   0) the ID for the parsed predicate (i.e., from MATCH)
                #   1) a dict with metric names as keys and type checking code as vals
                #   2) a list containing the new contents to add to the predicate
                parsed_pred = self._parse_binary_pred(pred)
            else:
                raise RuntimeError("Predicate not recognized as unary or binary")
            if parsed_pred is None:
                raise RedundantQueryFilterWarning("Parsed an empty predicate")
            if parsed_pred[0] not in self.predicates:
                self.predicates[parsed_pred[0]] = {
                    "metric_checks": {},
                    "pred": [],
                }
            for metric, check in parsed_pred[1].items():
                if metric in self.predicates[parsed_pred[0]]["metric_checks"]:
                    curr_check = self.predicates[parsed_pred[0]]["metric_checks"][metric]
                    if check != curr_check:
                        raise InvalidQueryFilter(
                            "Metric '{}' is treated as two seperate types".format(
                                metric
                            )
                        )
                    continue
                self.predicates[parsed_pred[0]]["metric_checks"][metric] = check
            self.predicates[parsed_pred[0]]["pred"].extend(parsed_pred[2])

    def _is_unary_pred(self, obj):
        return (
            cname(obj) in self._predicate_classes["unary"]
            or cname(obj) in self._predicate_classes["string"]
            or cname(obj) in self._predicate_classes["num"]
        )

    def _is_binary_pred(self, obj):
        return cname(obj) in self._predicate_classes["binary"]
    
    def _parse_unary_pred(self, obj):
        if cname(obj) == "NotPred":
            return self._parse_not_pred(obj)
        return self._parse_single_pred(obj)
    
    def _parse_not_pred(self, obj):
        parsed_subpred = self._parse_single_pred(obj.subpred)
        parsed_subpred[2].insert(0, "~")
        return parsed_subpred

    def _parse_binary_pred(self, obj):
        parsed_subpred = self._parse_unary_pred(obj.subpred)
        if cname(obj) == "AndPred":
            parsed_subpred[2].insert(0, "&")
        elif cname(obj) == "OrPred":
            parsed_subpred[2].insert(0, "|")
        else:
            raise RuntimeError("Binary pred is not recognized!")
        return parsed_subpred
    
    def _parse_single_pred(self, obj):
        obj_name = cname(obj)
        if obj_name in self._predicate_classes["string"]:
            return self._parse_string_pred(obj)
        if obj_name in self._predicate_classes["num"]:
            return self._parse_num_pred(obj)
        if obj_name == "NonePred":
            return self._parse_none_pred(obj, is_not=False)
        if obj_name == "NotNonePred":
            return self._parse_none_pred(obj, is_not=True)
        if obj_name == "LeafPred":
            return self._parse_leaf_pred(obj, is_not=False)
        if obj_name == "NotLeafPred":
            return self._parse_leaf_pred(obj, is_not=True)
        raise RuntimeError("Invalid single predicate detected")
    
    def _parse_none_pred(self, obj, is_not):
        parsed_pred = []
        if is_not:
            parsed_pred.append("~")
        if obj.metric == "depth":
            parsed_pred.append(
                "df.index.get_level_values(0).apply(lambda n: n._depth is None)"
            )
        elif obj.metric == "node_id":
            parsed_pred.append(
                "df.index.get_level_values(0).apply(lambda n: n._hatchet_nid is None)"
            )
        else:
            parsed_pred.append(
                "df[\"{}\"].apply(lambda x: x is None)".format(
                    obj.metric
                )
            )
        return (
            obj.name,
            {},
            parsed_pred
        )

    def _parse_leaf_pred(self, obj, is_not):
        parsed_pred = []
        if is_not:
            parsed_pred.append("~")
        parsed_pred.append(
            "df.index.get_level_values(0).apply(lambda n: len(n.children) == 0)"
        )
        return (
            obj.name,
            {},
            parsed_pred
        )
        
    def _parse_string_pred(self, obj):
        obj_name = cname(obj)
        if obj_name == "StringEq":
            return self._parse_string_eq(obj)
        if obj_name == "StringStartsWith":
            return self._parse_string_startswith(obj)
        if obj_name == "StringEndsWith":
            return self._parse_string_endswith(obj)
        if obj_name == "StringContains":
            return self._parse_string_contains(obj)
        if obj_name == "StringMatch":
            return self._parse_string_match(obj)
        raise RuntimeError("Invalid string predicate detected")
    
    def _parse_string_eq(self, obj):
        return (
            obj.name,
            {
                obj.metric: "is_string_dtype(df[\"{}\"])".format(obj.metric)
            },
            [
                "df[\"{}\"].apply(lambda s: s == \"{}\")".format(
                    obj.metric,
                    obj.val,
                )
            ]
        )
    
    def _parse_string_startswith(self, obj):
        return (
            obj.name,
            {
                obj.metric: "is_string_dtype(df[\"{}\"])".format(obj.metric)
            },
            [
                "df[\"{}\"].str.startswith(\"{}\")".format(
                    obj.metric,
                    obj.val,
                )
            ]
        )
    
    def _parse_string_endswith(self, obj):
        return (
            obj.name,
            {
                obj.metric: "is_string_dtype(df[\"{}\"])".format(obj.metric)
            },
            [
                "df[\"{}\"].str.endswith(\"{}\")".format(
                    obj.metric,
                    obj.val,
                )
            ]
        )
    
    def _parse_string_contains(self, obj):
        return (
            obj.name,
            {
                obj.metric: "is_string_dtype(df[\"{}\"])".format(obj.metric)
            },
            [
                "df[\"{}\"].str.contains(\"{}\")".format(
                    obj.metric,
                    obj.val,
                )
            ]
        )
    
    def _parse_string_match(self, obj):
        return (
            obj.name,
            {
                obj.metric: "is_string_dtype(df[\"{}\"])".format(obj.metric)
            },
            [
                "df[\"{}\"].str.match(r\"{}\\Z\")".format(
                    obj.metric,
                    obj.val,
                )
            ]
        )
    
    def _parse_num_pred(self, obj):
        obj_name = cname(obj)
        if obj_name == "NumEq":
            return self._parse_num_ineq(obj, eq_op="==")
        if obj_name == "NumLt":
            return self._parse_num_ineq(obj, eq_op="<")
        if obj_name == "NumGt":
            return self._parse_num_ineq(obj, eq_op=">")
        if obj_name == "NumLte":
            return self._parse_num_ineq(obj, eq_op="<=")
        if obj_name == "NumGte":
            return self._parse_num_ineq(obj, eq_op=">=")
        if obj_name == "NumNan":
            return self._parse_num_nan(obj, is_not=False)
        if obj_name == "NumNotNan":
            return self._parse_num_nan(obj, is_not=True)
        if obj_name == "NumInf":
            return self._parse_num_inf(obj, is_not=False)
        if obj_name == "NumNotInf":
            return self._parse_num_inf(obj, is_not=True)
        raise RuntimeError("Invalid numeric predicate detected")
            
    def _parse_num_ineq(self, obj, eq_op):
        return (
            obj.name,
            {
                obj.metric: "is_numeric_dtype(df[\"{}\"])".format(obj.metric)
            },
            [
                "df[\"{}\"] {} {}".format(
                    obj.metric,
                    eq_op,
                    obj.val,
                )
            ]
        )
    
    def _parse_num_nan(self, obj, is_not):
        parsed_pred = []
        if is_not:
            parsed_pred.append("~")
        parsed_pred.append(
            "np.isnan(df[\"{}\"])".format(obj.metric)
        )
        return (
            obj.name,
            {
                obj.metric: "is_numeric_dtype(df[\"{}\"])".format(obj.metric)
            },
            parsed_pred
        )
    
    def _parse_num_inf(self, obj, is_not):
        parsed_pred = []
        if is_not:
            parsed_pred.append("~")
        parsed_pred.append(
            "np.isinf(df[\"{}\"])".format(obj.metric)
        )
        return (
            obj.name,
            {
                obj.metric: "is_numeric_dtype(df[\"{}\"])".format(obj.metric)
            },
            parsed_pred
        )