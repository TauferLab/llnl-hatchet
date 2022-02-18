import igraph as ig

class Graph:

    def __init__(self, nodes, edges, roots):
        self.graph = ig.Graph(
            n=len(nodes),
            edges=edges,
            vertex_attrs=[{"frame": n} for n in nodes]
            directed=True,
        )
