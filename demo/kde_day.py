import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

term = sys.argv[1]

fig = plt.figure()

with open('/tmp/kde/part-00000', 'r') as in_file:
    densities = []
    for line in in_file:
        density = line[1:-2]
        densities.append(density)

days = np.arange(0, 7)
plt.plot(days, densities, '-.', label=term)
plt.xlabel('Day')
plt.ylabel('Density')
plt.xticks(days, ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'])
plt.grid()
plt.legend(prop={'size': 8})

plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.22),
           fancybox=True, shadow=True, ncol=4, prop={'size': 10})

fig.savefig("kde.png")
plt.show()