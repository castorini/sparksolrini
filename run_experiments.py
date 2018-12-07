import datetime
import pathlib
import subprocess
import sys
import re

if len(sys.argv) < 2:
    print("usage: python3 run_experiments.py <seq/solr/all>")
    sys.exit()

EX_TYPE = sys.argv[1]

if EX_TYPE != "seq" and EX_TYPE != "solr" and EX_TYPE != "all":
    print("usage: python3 run_experiments.py <seq/solr/all>")
    sys.exit()

LOG_DIR_NAME = '/hdd1/CS848-project/exp_results'

pathlib.Path(LOG_DIR_NAME).mkdir(parents=True, exist_ok=True)

search_terms = ["napoleon", "interpol", "belt", "kind", "idea", "current", "other", "public"]

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
        --index gov2"
}

def run_exp(type):
    for term in search_terms:
        print("\n" + str(datetime.datetime.now()) + " executing " + type + " with term : " + term)
        command = command_template[type].format(term)

        result = subprocess.check_output(command.split()).decode("utf-8")

        if type == "solr" :
            log_file_name = LOG_DIR_NAME+"/solr-spark-"+term+".log"
        elif type == "seq" :
            log_file_name = LOG_DIR_NAME+"/solr-seq-"+term+".log"

        run_time = 0

        with open(log_file_name, "w+") as f:
            for line in result.split("\n"):
                f.write(line+"\n")

                search = re.match('.*Took (.*)ms', line)
                if search:
                    run_time = search.group(1)

        print(str(datetime.datetime.now()) + " runtime of " + type + " with term : " + term + " - " + str(run_time) + " ms")

if (EX_TYPE == "all") :
    for key in command_template.keys():
        run_exp(key)
else:
    run_exp(EX_TYPE)
