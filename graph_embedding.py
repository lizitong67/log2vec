import networkx as nx
import dgl

graph = nx.read_gml("data/demo_data/graph.gml")
# g is an empty graph to make the graph_dgl's nodes start from 0 consecutively
g = nx.MultiGraph()
graph = nx.disjoint_union(graph, g)
graph_dgl = dgl.DGLGraph(graph)
print('We have %d nodes.' % graph_dgl.number_of_nodes())
# The number of edges of multi-graph in dgl is double
print('We have %d edges.' % graph_dgl.number_of_edges())

