import sys

if len(sys.argv) < 2:
    print("usage: python3 get_dist.py <file_name>")
    sys.exit()

mapping = {}

with open(sys.argv[1]) as f:
    for line in f.readlines():
        splits = line.rsplit(',', 1)
        if len(splits) > 0:
            count = int(splits[1])
            if count in mapping:
                mapping[count] += 1
            else:
                mapping[count] = 1

with open("dist.txt", "w") as f:
    for key in sorted(mapping):
        f.write(str(key) + "\t" + str(mapping[key]) + "\n")