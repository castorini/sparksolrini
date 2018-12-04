import datetime
import pathlib
import subprocess
import sys
import time


if len(sys.argv) < 4:
    print("usage: python3 collect_metrics.py <seq/solr/spark> <search term> <sleep time in sec>")
    sys.exit()

DIR_NAME = 'metrics'
EX_TYPE = sys.argv[1]
SEARCH_TERM = sys.argv[2]

if EX_TYPE != "seq" and EX_TYPE != "solr" and EX_TYPE != "spark":
    print("usage: python3 collect_metrics.py <seq/solr/spark> <search term> <sleep time in sec>")
    sys.exit()

pathlib.Path(DIR_NAME).mkdir(parents=True, exist_ok=True)

nodes_metrics_file = open(DIR_NAME+"/"+EX_TYPE+"_"+SEARCH_TERM+"_node.txt", "w+")
pods_metrics_file = open(DIR_NAME+"/"+EX_TYPE+"_"+SEARCH_TERM+"_pod.txt", "w+")

start_time = datetime.datetime.now()

while True:
    current_time = datetime.datetime.now()
    time_elapsed = (current_time - start_time).seconds
    top_nodes = subprocess.check_output(["kubectl", "top", "nodes"]).decode("utf-8")

    for line in top_nodes.split("\n")[1:]:
        if line != "":
            nodes_metrics_file.write(str(time_elapsed)+"\t"+line+"\n")

    get_pods = subprocess.check_output(["kubectl", "get", "pods", "-o", "wide"]).decode("utf-8")
    mapping = {}
    for line in get_pods.split("\n")[1:]:
        splits = line.split()
        if len(splits) > 6:
            mapping[splits[0]] = splits[6]

    top_pods = subprocess.check_output(["kubectl", "top", "pods"]).decode("utf-8")

    for line in top_pods.split("\n")[1:]:
        if line != "":
            splits = line.split()
            pods_metrics_file.write(str(time_elapsed)+"\t"+line+"\t" + mapping[splits[0]] + "\n")

    time.sleep(int(sys.argv[3]))
