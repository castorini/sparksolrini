import networkx as nx
import matplotlib.pyplot as plt

g = nx.read_edgelist('/tmp/link_analysis/part-00000', delimiter=';', create_using=nx.Graph(), nodetype=str)

print(nx.info(g))

nx.draw(g)

plt.show()
plt.savefig('/tmp/link_analysis/network_graph.png')
