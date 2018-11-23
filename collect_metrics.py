import time
import subprocess
import datetime
import pathlib

DIR_NAME = 'metrics'

pathlib.Path(DIR_NAME).mkdir(parents=True, exist_ok=True)

start_time = datetime.datetime.now()

nodes_metrics_file = open(DIR_NAME+"/nodes_"+str(start_time)+".txt", "w+")
pods_metrics_file = open(DIR_NAME+"/pods_"+str(start_time)+".txt", "w+")

while True:
    top_nodes = subprocess.check_output(["kubectl", "top", "nodes"])    
    current_time = datetime.datetime.now()
    time_elapsed = current_time - start_time
    
    nodes_metrics_file.write(str(time_elapsed)+"\n")
    nodes_metrics_file.write(top_nodes.decode("utf-8"))

    top_pods = subprocess.check_output(["kubectl", "top", "pods"])    
    current_time = datetime.datetime.now()
    time_elapsed = current_time - start_time
    
    pods_metrics_file.write(str(time_elapsed)+"\n")
    pods_metrics_file.write(top_pods.decode("utf-8"))

    time.sleep(10)
