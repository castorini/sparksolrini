import datetime
import pathlib
import subprocess
import sys
import time

if len(sys.argv) < 4:
    print("usage: python3 collect_driver_metrics.py <seq/solr/hdfs> <search term> <sleep time in sec>")
    sys.exit()

DIR_NAME = 'metrics'
EX_TYPE = sys.argv[1]
SEARCH_TERM = sys.argv[2]

if EX_TYPE != "seq" and EX_TYPE != "solr" and EX_TYPE != "hdfs":
    print("usage: python3 collect_driver_metrics.py <seq/solr/hdfs> <search term> <sleep time in sec>")
    sys.exit()

pathlib.Path(DIR_NAME).mkdir(parents=True, exist_ok=True)

start_time = datetime.datetime.now()

metrics_file = open(DIR_NAME+"/"+EX_TYPE+"_"+SEARCH_TERM+".txt", "w+")

while True:
    current_time = datetime.datetime.now()
    time_elapsed = (current_time - start_time).seconds
    usage = subprocess.check_output(["ps", "aux"]).decode("utf-8").split('\n')

    for process in usage:
        if "term "+SEARCH_TERM in process:
            metrics_file.write(str(time_elapsed)+"\t"+process+"\n")
            break

    metrics_file.flush()
    time.sleep(int(sys.argv[3]))
