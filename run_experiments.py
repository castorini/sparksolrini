import datetime
import pathlib
import subprocess
import sys
import re

# cp run_experiments.py, collect_metrics.py, collect_driver_metrics.py to ~/
# execute run_experiments.py
# metrics folder for cluster will be creaded under ~/

if len(sys.argv) < 2:
    print("usage: python3 run_experiments.py <seq/solr/hdfs/all>")
    sys.exit()

EX_TYPE = sys.argv[1]

if EX_TYPE != "seq" and EX_TYPE != "solr" and EX_TYPE != "hdfs" and EX_TYPE != "all":
    print("usage: python3 run_experiments.py <seq/solr/hdfs/all>")
    sys.exit()

LOG_DIR_NAME = 'metrics'

pathlib.Path(LOG_DIR_NAME).mkdir(parents=True, exist_ok=True)

search_terms = ["napoleon", "interpol", "belt", "kind", "idea", "study", "current", "more", "service", "other", "public"]

command_template = {
    'solr' : "spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
        --master k8s://http://192.168.152.201:8080 \
        --deploy-mode client \
        --name sent-detector-solr-spark \
        --class ca.uwaterloo.cs848.SolrSpark \
        --conf spark.driver.memory=16g \
        --conf spark.executor.memory=8g \
        --conf spark.executor.instances=5 \
        --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:cs848-nlp13 \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
        --conf spark.kubernetes.executor.limit.cores=12 \
        --conf spark.kubernetes.executor.request.cores=11 \
        --conf spark.executor.cores=11 \
        /hdd1/CS848-project/target/cs848-project-1.0-SNAPSHOT.jar \
        --term {0} \
        --field raw \
        --solr 192.168.152.201:32181 \
        --index gov2",
    'seq' : "java -Xms24g -Xmx30g -cp /hdd1/CS848-project/target/cs848-project-1.0-SNAPSHOT.jar \
        ca.uwaterloo.cs848.Solr \
        --term {0} \
        --field raw \
        --solr http://192.168.152.201:8983/solr,http://192.168.152.202:8983/solr,http://192.168.152.203:8983/solr,http://192.168.152.204:8983/solr,http://192.168.152.205:8983/solr \
        --index gov2",
    'hdfs' : "spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
        --master k8s://http://192.168.152.201:8080 \
        --deploy-mode client \
        --name sent-detector-hdfs-spark \
        --class ca.uwaterloo.cs848.HdfsSpark \
        --conf spark.driver.memory=16g \
        --conf spark.executor.memory=8g \
        --conf spark.executor.instances=5 \
        --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:cs848-nlp13 \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
        --conf spark.kubernetes.executor.limit.cores=12 \
        --conf spark.kubernetes.executor.request.cores=11 \
        --conf spark.executor.cores=11 \
        /hdd1/CS848-project/target/cs848-project-1.0-SNAPSHOT.jar \
        --path hdfs://192.168.152.203/gov2/gov2-corpus \
        --term {0}"
}

def run_exp(type):
    for term in search_terms:
        print("\n" + str(datetime.datetime.now()) + " executing " + type + " with term : " + term)
        command = command_template[type].format(term)

        driver_metric_proc = subprocess.Popen(["python3", "collect_driver_metrics.py", type, term, "60"])
        cluster_metric_proc = subprocess.Popen(["ssh", "tem101", "python3 collect_metrics.py " + type + " " + term + " 60"])

        print(str(datetime.datetime.now()) + " driver metrics pid : " + str(driver_metric_proc.pid))
        print(str(datetime.datetime.now()) + " cluster metrics pid : " + str(cluster_metric_proc.pid))

        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout_data, stderr_data = process.communicate()
        stdout_data = stdout_data.decode("utf-8")
        stderr_data = stderr_data.decode("utf-8")

        driver_metric_proc.kill()
        cluster_metric_proc.kill()

        if len(stderr_data) > 0:
            print(str(datetime.datetime.now()) + " error has occurred")
            for line in stderr_data.split('\n'):
                print(line)

        if type == "solr" :
            log_file_name = LOG_DIR_NAME+"/solr-spark-"+term+".log"
        elif type == "seq" :
            log_file_name = LOG_DIR_NAME+"/solr-seq-"+term+".log"
        elif type == "hdfs" :
            log_file_name = LOG_DIR_NAME+"/hdfs-spark-"+term+".log"

        run_time = 0

        with open(log_file_name, "w+") as f:
            for line in stdout_data.split('\n'):
                f.write(line)

                search = re.match('.*Took (.*)ms', line)
                if search:
                    run_time = search.group(1)

        print(str(datetime.datetime.now()) + " runtime of " + type + " with term : " + term + " - " + str(run_time) + " ms")

if (EX_TYPE == "all") :
    for key in command_template.keys():
        run_exp(key)
else:
    run_exp(EX_TYPE)
