import pandas as pd

from .errors import InvalidQueryFilter
from ..node import Node, traversal_order
from .query import Query
from .compound import CompoundQuery
from .object_dialect import ObjectQuery
from .string_dialect import parse_string_dialect


class QueryEngine:
    def __init__(self, predicate_mode="row"):
        self.predicate_cache = []
        self.set_predicate_mode(predicate_mode)

    def set_predicate_mode(self, mode):
        if mode not in ("row", "col"):
            raise ValueError("'predicate_mode' must be one of 'row' or 'col'")
        self.predicate_mode = mode

    def reset_cache(self):
        self.predicate_cache = []

    def apply(self, query, graph, dframe):
        pass

    def _eval_predicates(self, query, dframe):
        self.reset_cache()
        self.predicate_cache = [[] for _ in len(query)]
        node_idx = dframe.index
        if isinstance(dframe.index, pd.MultiIndex):
            node_idx = dframe.index.get_level_values("node")
        if self.predicate_mode == "row":
            for node in node_idx:
                for i, query_node in enumerate(query):
                    _, pred = query_node
                    if isinstance(dframe.index, pd.MultiIndex):
                        row = dframe.xs(node, level="node", drop_level=False)
                    else:
                        row = dframe.loc[node]
                    if pred(row):
                        self.predicate_cache[i].append(node)
        else self.predicate_mode == "col":
            pred_results = pd.DataFrame(index=node_idx)
            for i, query_node in enumerate(query):
                _, pred = query_node
                pred_results["pred_{}".format(i)] = pred(dframe)
                self.predicate_cache[i] = pred_results.index[
                    pred_results["pred_{}".format(i)]
                ].tolist()
        return self.predicate_cache
    
    def _search_for_matches(self, query, dframe, graph):
        pass
