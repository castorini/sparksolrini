import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import math
import re
import sys
import os

root_dir = os.getcwd()
graphs_dir = os.path.join(root_dir, "graphs")

terms = ["idea", "good", "intern", "event", "start", "end"]

# 3ms
# exp1_runtime = [11.7, 23.3, 33.4, 46.0, 58.4, 68.1]
# exp2_runtime = [3.3, 6.2, 9.3, 12.0, 15.8, 18.8]
# exp3_runtime = [13.9, 16.0, 25.7, 28.8, 31.9, 42.9]

# 9ms
exp1_runtime = [13.8, 27.1, 41.6, 52.4, 66.5, 80.4]
exp2_runtime = [7.3, 14.0, 19.2, 24.4, 30.9, 37.8]
exp3_runtime = [22.0, 30.8, 48.7, 51.4, 55.6, 74.6]

X = np.arange(len(terms))

plt.bar(X - 0.27, exp1_runtime, color='y', width=0.25)

plt.bar(X, exp2_runtime, color='g', width=0.25)

plt.bar(X + 0.27, exp3_runtime, color='b',width=0.25)

plt.xticks(X, terms)
plt.xlabel('Search Term')
plt.ylabel('Execution Time (m)')
plt.legend(['ParallelDocId', 'SolrRdd', 'Hdfs'], loc='upper left')

plt.savefig(os.path.join(graphs_dir, "runtime_selectivity_9ms.png"))
