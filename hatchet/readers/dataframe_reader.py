# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import hatchet.graphframe
from hatchet.node import Node
from hatchet.graph import Graph

import pandas as pd

from abc import abstractmethod
from typing import Dict, List

# TODO The ABC class was introduced in Python 3.4.
# When support for earlier versions is (eventually) dropped,
# this entire "try-except" block can be reduced to:
# from abc import ABC
try:
    from abc import ABC
except ImportError:
    from abc import ABCMeta

    ABC = ABCMeta("ABC", (object,), {"__slots__": ()})


def _get_node_from_df_iloc(df: pd.DataFrame, ind: int) -> Node:
    node = None
    if isinstance(df.iloc[ind].name, tuple):
        node = df.iloc[ind].name[0]
    elif isinstance(df.iloc[ind].name, Node):
        node = df.iloc[ind].name
    else:
        raise InvalidDataFrameIndex(
            "DataFrame index elements must be either a tuple or a Node"
        )
    return node


def _get_parents_and_children(df: pd.DataFrame) -> Dict[Node, Dict[str, List[int]]]:
    rel_dict = {}
    for i in range(len(df)):
        node = _get_node_from_df_iloc(df, i)
        if node not in rel_dict:
            rel_dict[node] = {}
            rel_dict[node]["parents"] = df.iloc[i].loc["parents"]
            rel_dict[node]["children"] = df.iloc[i].loc["children"]
    return rel_dict


def _reconstruct_graph(
    df: pd.DataFrame, rel_dict: Dict[Node, Dict[str, List[int]]]
) -> Graph:
    node_list = sorted(list(df.index.to_frame()["node"]))
    for i in range(len(df)):
        node = _get_node_from_df_iloc(df, i)
        if len(node.children) == 0:
            node.children = [node_list[nid] for nid in rel_dict[node]["children"]]
        if len(node.parents) == 0:
            node.parents = [node_list[nid] for nid in rel_dict[node]["parents"]]
    roots = [node for node in node_list if len(node.parents) == 0]
    return Graph(roots)


class DataframeReader(ABC):
    """Abstract Base Class for reading in checkpointing files."""

    def __init__(self, filename: str) -> None:
        self.filename = filename

    @abstractmethod
    def _read_dataframe_from_file(self, **kwargs) -> pd.DataFrame:
        pass

    def read(self, **kwargs) -> hatchet.graphframe.GraphFrame:
        df = self._read_dataframe_from_file(**kwargs)
        rel_dict = _get_parents_and_children(df)
        graph = _reconstruct_graph(df, rel_dict)
        graph.enumerate_traverse()
        exc_metrics = df.iloc[0, df.columns.get_loc("exc_metrics")]
        inc_metrics = df.iloc[0, df.columns.get_loc("inc_metrics")]
        default_metric = df.iloc[0, df.columns.get_loc("default_metric")]
        df.drop(
            columns=[
                "children",
                "parents",
                "exc_metrics",
                "inc_metrics",
                "default_metric",
            ],
            inplace=True,
        )
        return hatchet.graphframe.GraphFrame(
            graph,
            df,
            exc_metrics=exc_metrics,
            inc_metrics=inc_metrics,
            default_metric=default_metric,
        )


class InvalidDataFrameIndex(Exception):
    """Raised when the DataFrame index is of an invalid type."""
