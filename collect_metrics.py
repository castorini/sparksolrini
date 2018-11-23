import datetime
import pathlib
import subprocess
import sys
import time

if len(sys.argv) < 2:
    print("usage: python3 collect_metrics.py <sleep time in sec>")
    sys.exit()

DIR_NAME = 'metrics'

pathlib.Path(DIR_NAME).mkdir(parents=True, exist_ok=True)

start_time = datetime.datetime.now()

nodes_metrics_file = open(DIR_NAME+"/nodes_"+str(start_time)+".txt", "w+")
pods_metrics_file = open(DIR_NAME+"/pods_"+str(start_time)+".txt", "w+")

while True:
    current_time = datetime.datetime.now()
    time_elapsed = (current_time - start_time).seconds
    top_nodes = subprocess.check_output(["kubectl", "top", "nodes"]).decode("utf-8")
    
    for line in top_nodes.split("\n"):
        if line != "":
            nodes_metrics_file.write(str(time_elapsed)+"\t"+line+"\n")

    top_pods = subprocess.check_output(["kubectl", "top", "pods"]).decode("utf-8")

    for line in top_pods.split("\n"):
        if line != "":
            pods_metrics_file.write(str(time_elapsed)+"\t"+line+"\n")

    time.sleep(int(sys.argv[1]))
