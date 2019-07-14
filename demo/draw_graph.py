import networkx as nx
import matplotlib.pyplot as plt

g = nx.read_edgelist('/tmp/link_analysis/part-00000', delimiter=';', create_using=nx.Graph(), nodetype=str)

print(nx.info(g))

nx.draw_networkx(g, arrows=True, node_size=20, with_labels=False)

plt.show()
plt.savefig('network_graph.png')