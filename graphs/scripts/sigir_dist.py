import matplotlib.pyplot as plt
import numpy as np
import math

mapping = {}

graph_freq = 10
x = []
y = []

with open("dist.txt") as f:
    for line in f.readlines():
        splits = line[1:-2].split(",")
        freq = float(splits[0])
        count = float(splits[1])
        print(freq)
        print(count)
        x.append(freq)
        y.append(count)
        # if count > 150 and freq > 10:
        #     x.append(freq)
        #     y.append(count)

delta = math.floor(len(x) / graph_freq)

x = x[0::delta]
y = y[0::delta]

plt.subplots_adjust(left=0.2)
plt.plot(x, y)
plt.xlabel('Document frequency', fontsize='large')
plt.ylabel('Number of words', fontsize='large')
plt.savefig("Document_distribution2.png")
# plt.show()
