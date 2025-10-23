# Copyright 2017-2025 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re
from functools import partial
from typing import Union

import pandas as pd

from hatchet.node import Node

from .query import Query


def match_subtree_by_root(
    root_name_regex=None, root_predicate=None, context_depth=0, multi_index_mode="off"
) -> Query:
    """Generate a query that matches a subtree/subgraph based off its root node.

    Args:
        root_name_regex (str, optional): A regex that the root node's name should match or string that the name should equal. Defaults to None.
        root_predicate (callable, optional): A predicate (i.e., callable) that returns true for the root node. Defaults to None.
        context_depth (int, optional): The number of levels in the tree/graph above the root node to keep as context. Defaults to 0.
        multi_index_mode (str, optional): The aggreator to use when there is a multi-index in the DataFrame. Can be one of "off" (no multi-index support), "all" (all rows in multi-index must satisfy predicate), or "any" (any row in multi-index must satisfy predicate). Defaults to "off".

    Raises:
        RuntimeError: when neither "root_name_regex" or "root_predicate" are provided or when both are provided
        TypeError: when "context_depth" is not an int
        ValueError: when "multi_index_mode" is none of "off", "any", or "all"
        TypeError: when "root_name_regex" is not a string
        TypeError: when "root_predicate" is not a callable

    Returns:
        Query: the generated query that matches a subtree/subgraph based off its root node
    """
    if (
        root_predicate is None and (root_name_regex is None or root_name_regex == "")
    ) or (
        root_predicate is not None
        and (root_name_regex is not None and root_name_regex != "")
    ):
        raise RuntimeError(
            "Exactly one of 'root_name_regex' or 'root_predicate' must be provided"
        )
    if not isinstance(context_depth, int):
        raise TypeError("The 'context_depth' parameter must be an integer")
    if multi_index_mode not in ("off", "all", "any"):
        raise ValueError(
            "The 'multi_index_mode' parameter must be one of 'off' (no multi-index support), 'all' (all rows in multi-index must satisfy predicate), or 'any' (any row in multi-index must satisfy predicate)"
        )

    def name_regex_predicate(row_data) -> bool:
        if multi_index_mode in ("all", "any") and not isinstance(
            row_data, pd.DataFrame
        ):
            raise TypeError(
                f"The 'multi_index_mode' parameter was set to '{multi_index_mode}', but the data passed to the auto-generated predicate is not a DataFrame. Are you sure your data includes a multi-index?"
            )
        if multi_index_mode == "off" and not isinstance(row_data, pd.Series):
            raise TypeError(
                "The 'multi_index_mode' parameter was set to 'off', but the data passed to the auto-generated predicate is not a pandas Series. Are you sure your data does not include a multi-index?"
            )
        if multi_index_mode == "off":
            return re.match(root_name_regex, row_data["name"]) is not None
        regex_series = row_data["name"].str.match(root_name_regex)
        if multi_index_mode == "all":
            return regex_series.all()
        return regex_series.any()

    local_root_predicate = root_predicate
    if root_predicate is None:
        if not isinstance(root_name_regex, str):
            raise TypeError("The 'root_name_regex' parameter must be a string")
        local_root_predicate = name_regex_predicate
    if not isinstance(local_root_predicate, callable):
        raise TypeError(
            "Either 'root_predicate' was provided but is not a callable, or there was an internal error in generating the predicate"
        )
    query = Query()
    if context_depth == 0:
        query.match(".", local_root_predicate)
    else:
        query.match(context_depth).rel(".", local_root_predicate)
    query.rel("*")
    return query


def match_leaves() -> Query:
    """Generate a query that matches all leaf nodes.

    Returns:
        Query: the generate query
    """

    def is_leaf_predicate(row_data) -> bool:
        if isinstance(row_data, pd.DataFrame):
            return len(row_data.index.get_level_values("node")[0].children) == 0
        elif isinstance(row_data, pd.Series):
            return len(row_data.name.children) == 0
        else:
            raise TypeError(
                "Row data passed into a predicate should only be a pandas DataFrame (for multi-indexed data) or a pandas Series (for non-multi-indexed data)"
            )

    return Query().match(".", is_leaf_predicate)


def trim_top_levels(num_levels: int) -> Query:
    """Trims the top N levels off the top of the tree/graph.

    Args:
        num_levels (int): the number of levels to trim

    Raises:
        TypeError: when "num_levels" is not an int
        ValueError: when "num_levels" is non-positive

    Returns:
        Query: the generated query that trims the top N levels off the top of the tree/graph
    """
    if not isinstance(num_levels, int):
        raise TypeError("The 'num_levels' parameter must be an integer")
    elif num_levels < 0:
        raise ValueError("The 'num_levels' parameter must be a positive integer")
    elif num_levels == 0:
        raise ValueError(
            "Since the 'num_levels' parameter is 0, the resulting query would do nothing"
        )

    def is_not_in_top_levels_predicate(row_data) -> bool:
        if isinstance(row_data, pd.DataFrame):
            return row_data.index.get_level_values("node")[0]._depth >= num_levels
        elif isinstance(row_data, pd.Series):
            return row_data.name._depth >= num_levels
        else:
            raise TypeError(
                "Row data passed into a predicate should only be a pandas DataFrame (for multi-indexed data) or a pandas Series (for non-multi-indexed data)"
            )

    return Query().match(".", is_not_in_top_levels_predicate)


def match_by_metric_condition(
    metric_name: str, metric_condition: callable, multi_index_mode="off"
) -> Query:
    """Match all nodes where the specified metric satisfies the specified callable.

    Args:
        metric_name (str): the name of the metric
        metric_condition (callable): a callable that returns True when the metric value satisfies the desired condition
        multi_index_mode (str, optional): The aggreator to use when there is a multi-index in the DataFrame. Can be one of "off" (no multi-index support), "all" (all rows in multi-index must satisfy predicate), or "any" (any row in multi-index must satisfy predicate). Defaults to "off".

    Raises:
        TypeError: when "metric_name" is not a string
        TypeError: when "metric_condition" is not a callable
        ValueError: when "multi_index_mode" is none of "off", "any", or "all"

    Returns:
        Query: the generated query that matches all nodes where the metric satisfies the callable
    """
    if not isinstance(metric_name, str):
        raise TypeError("The 'metric_name' parameter must be a string")
    elif not isinstance(metric_condition, callable):
        raise TypeError("The 'metric_condition' parameter must be a callable")
    if multi_index_mode not in ("off", "all", "any"):
        raise ValueError(
            "The 'multi_index_mode' parameter must be one of 'off' (no multi-index support), 'all' (all rows in multi-index must satisfy predicate), or 'any' (any row in multi-index must satisfy predicate)"
        )

    def metric_condition_predicate(row_data) -> bool:
        if multi_index_mode == "off" or isinstance(row_data, pd.Series):
            return metric_condition(row_data[metric_name])
        elif isinstance(row_data, pd.DataFrame):
            condition_series = row_data[metric_name].apply(
                lambda elem: metric_condition(elem)
            )
            if multi_index_mode == "all":
                return condition_series.all()
            return condition_series.any()
        else:
            raise TypeError(
                "Row data passed into a predicate should only be a pandas DataFrame (for multi-indexed data) or a pandas Series (for non-multi-indexed data)"
            )

    return Query().match(".", metric_condition_predicate)


def _match_node_object(node_obj: Node, row_data) -> bool:
    if isinstance(row_data, pd.DataFrame):
        return row_data.index.get_level_values("node")[0] == node_obj
    elif isinstance(row_data, pd.DataFrame):
        return row_data.name == node_obj
    else:
        raise TypeError(
            "Row data passed into a predicate should only be a pandas DataFrame (for multi-indexed data) or a pandas Series (for non-multi-indexed data)"
        )


def get_paths_between_nodes(
    src: Union[Node, callable], dst: Union[Node, callable]
) -> Query:
    """Get all paths between two nodes.

    Args:
        src (Union[Node, callable]): the source node, or a predicate (i.e., callable) that matches the source node
        dst (Union[Node, callable]): the destination node, or a predicate (i.e., callable) that matches the destination node

    Raises:
        TypeError: if "src" is neither a Node or a predicate
        TypeError: if "dst" is neither a Node or a predicate

    Returns:
        Query: the generated query that gets all paths between the nodes
    """
    if not isinstance(src, Node) and not isinstance(src, callable):
        raise TypeError(
            "The 'src' parameter must be either a hatchet.Node or a predicate (i.e., Python callable) representing the source node"
        )
    if not isinstance(dst, Node) and not isinstance(dst, callable):
        raise TypeError(
            "The 'dst' parameter must be either a hatchet.Node or a predicate (i.e., Python callable) representing the destination node"
        )

    query = Query()
    if isinstance(src, Node):
        query.match(".", partial(_match_node_object, node_obj=src))
    else:
        query.match(".", src)
    query.rel("*")
    if isinstance(dst, Node):
        query.rel(".", partial(_match_node_object, node_obj=src))
    else:
        query.rel(".", dst)
    return query


def get_paths_through_node(node: Union[Node, callable]) -> Query:
    """Get all paths that pass through the given node

    Args:
        node (Union[Node, callable]): the node, or a predicate (i.e., callable) that matches the node

    Raises:
        TypeError: if "node" is neither a Node or a predicate

    Returns:
        Query: the generated query that gets all paths between the nodes
    """
    if not isinstance(node, Node) and not isinstance(node, callable):
        raise TypeError(
            "The 'node' parameter must be either a hatchet.Node or a predicate (i.e., Python callable) representing the node"
        )

    query = Query()
    query.match("*")
    if isinstance(node, Node):
        query.rel(".", partial(_match_node_object, node_obj=node))
    else:
        query.rel(".", node)
    query.rel("*")
