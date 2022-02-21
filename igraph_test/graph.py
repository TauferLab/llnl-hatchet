import igraph as ig

def traversal_order(current_igraph, node):
    return (current_igraph.vs[node]["frame"], node)

class Graph:

    def __init__(self, node_frames, edges, roots, depths=None):
        self.graph = ig.Graph(
            directed=True,
        )
        igraph_vertices = []
        for frame in node_frames:
            igraph_vertex = self.graph.add_vertex(name=frame, depth=-1, frame=frame)
            igraph_vertices.append(igraph_vertex["name"])
        igraph_edges = [(igraph_vertex[i], igraph_vertex[j]) for i, j in edges]
        self.graph.add_edges(igraph_edges)
        if depths is None:
            self._enumerate_depth()
        else:
            try:
                _ = iter(depths)
            except TypeError:
                raise
            else:
                for i, d in enumerate(depths):
                    self.graph.vs[i]["depth"] = d
        self.roots = roots

    # TODO Add the "order" parameter back when I figure out how to do post-order traversal
    def traverse(self, attrs=None, visisted=None):
        if order not in ("pre", "post"):
            raise ValueError("order must be one of 'pre' or 'post'")
        if visited is None:
            visited = {}

        def value(nid):
            return nid if attrs is None else self.graph.vs[nid]["frame"].values(attrs)

        for root in sorted(self.roots, lambda x: traversal_order(self.graph, x)):
            for node_id in self.graph.dfsiter(root):
                if node_id in visisted:
                    visisted[node_id] += 1
                    continue
                visisted[node_id] = 1
                yield value(node_id)

    def is_tree(self):
        return self.graph.is_tree()

    def find_merges(self):
        merges = {}  # old_node -> merged_node
        inverted_merges = defaultdict(
            lambda: []
        )  # merged_node -> list of corresponding old_nodes
        processed = []

        def _find_child_merges(node_list):
            index = index_by("frame", node_list)
            for frame, children in index.items():
                if len(children) > 1:
                    min_id = min(children, key=id)
                    for child in children:
                        prev_min = merges.get(child, min_id)
                        # Get the new merged_node
                        curr_min = min([min_id, prev_min], key=id)
                        # Save the new merged_node to the merges dict
                        # so that the merge can happen later.
                        merges[child] = curr_min
                        # Update inverted_merges to be able to set node_list
                        # to the right value.
                        inverted_merges[curr_min].append(child)

        _find_child_merges(self.roots)
        for node in self.traverse():
            if node in processed:
                continue
            nodes = None
            # If node is going to be merged with other nodes,
            # collect the set of those nodes' children. This is
            # done to ensure that equivalent children of merged nodes
            # also get merged.
            if node in merges:
                new_node = merges[node]
                nodes = []
                for node_to_merge in inverted_merges[new_node]:
                    nodes.extend(node_to_merge.children)
                processed.extend(inverted_merges[new_node])
            # If node is not going to be merged, simply get the list of
            # node's children.
            else:
                nodes = node.children
                processed.append(node)
            _find_child_merges(nodes)

        return merges
