import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

term = sys.argv[1]

fig = plt.figure()

with open('/tmp/kde/part-00000', 'r') as in_file:
    densities = []
    for line in in_file:
        density = line
        densities.append(float(density))
        
hours = np.arange(0, 24)
plt.plot(hours, densities, '-.', label=term)
plt.xlabel('Hour')
plt.ylabel('Density')
plt.xticks(hours[1::3])
plt.title('Term: {}'.format(term))
plt.grid()
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.22),
               fancybox=True, shadow=True, ncol=4, prop={'size': 10})

plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.22),
           fancybox=True, shadow=True, ncol=4, prop={'size': 10})

fig.savefig("kde.png")
plt.show()