# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re
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
            
            return _predicate
            
        for quantifier, id in self.quantifiers:
            if id is None:
                self._add_node(
                    quantifer=quantifier,
                    predicate=_predicate_builder({}, [
                        "pd.Series([True]", "*", "len(df),", "dtype=bool,", "index=df.index)"
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
            new_node = [n.quant, n.name]
            if (n.quant is None
                    or n.quant.quantifier == ""
                    or n.quant.quantifier == 0):
                new_node[0] = "."
            else:
                new_node[0] = n.quant.quantifier
            self.quantifiers.append(tuple(new_node))

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


def parse_string_dialect(query_str):
    """Parse all types of String-based queries, including multi-queries that leverage
    the curly brace delimiters.

    Arguments:
        query_str (str): the String-based query to be parsed

    Returns:
        (Query or CompoundQuery): A Hatchet query object representing the String-based query
    """
    # TODO Check if there's a way to prevent curly braces in a string
    #      from being captured

    # Find the number of curly brace-delimited regions in the query
    query_str = query_str.strip()
    curly_brace_elems = re.findall(r"\{(.*?)\}", query_str)
    num_curly_brace_elems = len(curly_brace_elems)
    # If there are no curly brace-delimited regions, just pass the query
    # off to the CypherQuery constructor
    if num_curly_brace_elems == 0:
        if sys.version_info[0] == 2:
            query_str = query_str.decode("utf-8")
        return StringQuery(query_str)
    # Create an iterator over the curly brace-delimited regions
    curly_brace_iter = re.finditer(r"\{(.*?)\}", query_str)
    # Will store curly brace-delimited regions in the WHERE clause
    condition_list = None
    # Will store curly brace-delimited regions that contain entire
    # mid-level queries (MATCH clause and WHERE clause)
    query_list = None
    # If entire queries are in brace-delimited regions, store the indexes
    # of the regions here so we don't consider brace-delimited regions
    # within the already-captured region.
    query_idxes = None
    # Store which compound queries to apply to the curly brace-delimited regions
    compound_ops = []
    for i, match in enumerate(curly_brace_iter):
        # Get the substring within curly braces
        substr = query_str[match.start() + 1 : match.end() - 1]
        substr = substr.strip()
        # If an entire query (MATCH + WHERE) is within curly braces,
        # add the query to "query_list", and add the indexes corresponding
        # to the query to "query_idxes"
        if substr.startswith("MATCH"):
            if query_list is None:
                query_list = []
            if query_idxes is None:
                query_idxes = []
            query_list.append(substr)
            query_idxes.append((match.start(), match.end()))
        # If the curly brace-delimited region contains only parts of a
        # WHERE clause, first, check if the region is within another
        # curly brace delimited region. If it is, do nothing (it will
        # be handled later). Otherwise, add the region to "condition_list"
        elif re.match(r"[a-zA-Z0-9_]+\..*", substr) is not None:
            is_encapsulated_region = False
            if query_idxes is not None:
                for s, e in query_idxes:
                    if match.start() >= s or match.end() <= e:
                        is_encapsulated_region = True
                        break
            if is_encapsulated_region:
                continue
            if condition_list is None:
                condition_list = []
            condition_list.append(substr)
        # If the curly brace-delimited region is neither a whole query
        # or part of a WHERE clause, raise an error
        else:
            raise ValueError("Invalid grouping (with curly braces) within the query")
        # If there is a compound operator directly after the curly brace-delimited region,
        # capture the type of operator, and store the type in "compound_ops"
        if i + 1 < num_curly_brace_elems:
            rest_substr = query_str[match.end() :]
            rest_substr = rest_substr.strip()
            if rest_substr.startswith("AND"):
                compound_ops.append("AND")
            elif rest_substr.startswith("OR"):
                compound_ops.append("OR")
            elif rest_substr.startswith("XOR"):
                compound_ops.append("XOR")
            else:
                raise ValueError("Invalid compound operator type found!")
    # Each call to this function should only consider one of the full query or
    # WHERE clause versions at a time. If both types were captured, raise an error
    # because some type of internal logic issue occured.
    if condition_list is not None and query_list is not None:
        raise ValueError(
            "Curly braces must be around either a full mid-level query or a set of conditions in a single mid-level query"
        )
    # This branch is for the WHERE clause version
    if condition_list is not None:
        # Make sure you correctly gathered curly brace-delimited regions and
        # compound operators
        if len(condition_list) != len(compound_ops) + 1:
            raise ValueError(
                "Incompatible number of curly brace elements and compound operators"
            )
        # Get the MATCH clause that will be shared across the subqueries
        match_comp_obj = re.search(r"MATCH\s+(?P<match_field>.*)\s+WHERE", query_str)
        match_comp = match_comp_obj.group("match_field")
        # Iterate over the compound operators
        full_query = None
        for i, op in enumerate(compound_ops):
            # If in the first iteration, set the initial query as a CypherQuery where
            # the MATCH clause is the shared match clause and the WHERE clause is the
            # first curly brace-delimited region
            if i == 0:
                query1 = "MATCH {} WHERE {}".format(match_comp, condition_list[i])
                if sys.version_info[0] == 2:
                    query1 = query1.decode("utf-8")
                full_query = StringQuery(query1)
            # Get the next query as a CypherQuery where
            # the MATCH clause is the shared match clause and the WHERE clause is the
            # next curly brace-delimited region
            next_query = "MATCH {} WHERE {}".format(match_comp, condition_list[i + 1])
            if sys.version_info[0] == 2:
                next_query = next_query.decode("utf-8")
            next_query = StringQuery(next_query)
            # Add the next query to the full query using the compound operator
            # currently being considered
            if op == "AND":
                full_query = full_query & next_query
            elif op == "OR":
                full_query = full_query | next_query
            else:
                full_query = full_query ^ next_query
        return full_query
    # This branch is for the full query version
    else:
        # Make sure you correctly gathered curly brace-delimited regions and
        # compound operators
        if len(query_list) != len(compound_ops) + 1:
            raise ValueError(
                "Incompatible number of curly brace elements and compound operators"
            )
        # Iterate over the compound operators
        full_query = None
        for i, op in enumerate(compound_ops):
            # If in the first iteration, set the initial query as the result
            # of recursively calling this function on the first curly brace-delimited region
            if i == 0:
                full_query = parse_string_dialect(query_list[i])
            # Get the next query by recursively calling this function
            # on the next curly brace-delimited region
            next_query = parse_string_dialect(query_list[i + 1])
            # Add the next query to the full query using the compound operator
            # currently being considered
            if op == "AND":
                full_query = full_query & next_query
            elif op == "OR":
                full_query = full_query | next_query
            else:
                full_query = full_query ^ next_query
        return full_query