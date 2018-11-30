# CS848-project

---

## Spark

### Submitting Spark job

1) download spark from http://apache.forsale.plus/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz and unzip

2) build jar by running `mvn clean package` under `CS848-project`

3) add your jar to `spark-2.4.0-bin-hadoop2.7/examples/jars`

4) build docker image of spark using `./bin/docker-image-tool.sh -r <docker_hub_id> -t <tag_name> build`

5) push docker image of spark using `./bin/docker-image-tool.sh -r <docker_hub_id> -t <tag_name> push`

6) ssh into tem101 (or tem102)

7) download spark from http://apache.forsale.plus/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz and unzip on tembo

8) run job with following command format
&nbsp;&nbsp;&nbsp;&nbsp; from tem101 (cluster mode)
```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode cluster \
    --name <job_name> \
    --class <class_name> \
    --conf spark.driver.memory=5g \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<image_name>:<image_tag> \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/<jar_name>.jar <input>
```

&nbsp;&nbsp;&nbsp;&nbsp; note that class_name, image_name, image_tag, jar_name must be correctly provided. `/opt/spark/examples/jars/<jar_name>.jar` is location of target jar in the docker image

&nbsp;&nbsp;&nbsp;&nbsp; to run WordCount example,
```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode cluster \
    --name word-count \
    --class cs848.wordcount.WordCount \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=ljj7975/spark:ip \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
```

&nbsp;&nbsp;&nbsp;&nbsp; `192.168.152.202` can also be used

8) Once driver status has changed to Completed, `kubectl logs <spark_driver_pod_id>` to see logs


Refer following links for detail
- https://spark.apache.org/docs/latest/running-on-kubernetes.html
- https://weidongzhou.wordpress.com/2018/04/29/running-spark-on-kubernetes/

---
### Client mode

To use client mode, jar must be distributed by driver using `setJars` with location of jars inside docker image as following

```
val conf = new SparkConf()
  .setAppName("WordCount")
  .setJars(Array("/opt/spark/examples/jars/<jar_name>.jar"))
```

command is same except the job must be submitted through kubernete api server (http://192.168.152.201:8080)
```
bin/spark-submit \
    --master k8s://http://192.168.152.201:8080 \
    --deploy-mode client \
    --name <job_name> \
    --class <class_name> \
    --conf spark.driver.memory=5g \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<image_name>:<image_tag> \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    <path_to_the_jar>/<jar_name>.jar <input>
```
---

### Accessing files in HDFS through Spark

Must contact namenode which runs on tem103 (192.168.152.203).
Therefore, url is `hdfs://192.168.152.203/<path_to_file>` as in `sc.textFile("hdfs://192.168.152.203/test/20/0005447.xml")`

Noe that providing machine name like `node3` does not work.

---
## Running metric collector

To collect CPU and RAM usage on kubernete run following command on tem 101.
```
python3 collect_metrics.py <sleep time in sec>
```
the script will create two .txt files under `metrics` directory, one for nodes, one for pods.
each file will have start timestamp as part of its name.

Similarly, to collect resource usage of driver running on tem127,
```
python3 collect_driver_metrics.py <seq/solr/spark> <search term> <sleep time in sec>
```

## OpenNLP

### Gov2

Run ```sudo docker pull zeynepakkalyoncu/spark:cs848-nlp13``` to get the latest image

Navigate to the Spark root directory

Make sure ```log4j.properties``` is in the current directory

#### Experiment #1: SolrSeq

```
java -cp /opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
    cs848.nlp.SolrSeq \
    --term <search-term> \
    --field raw \
    --solr http://192.168.152.201:8983/solr,http://192.168.152.202:8983/solr,http://192.168.152.203:8983/solr,http://192.168.152.204:8983/solr,http://192.168.152.205:8983/solr \
    --index gov2 \
    [--debug] \
    [&> solr-seq-output.log]
```

#### Experiment #2: SolrSpark

```
bin/spark-submit \
    --master k8s://http://192.168.152.201:8080 \
    --deploy-mode client \
    --name sent-detector-solr-spark \
    --class cs848.nlp.SolrSpark \
    --conf spark.driver.memory=24g \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:cs848-nlp13 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
    --term <search-term> \
    --field raw \
    --solr http://192.168.152.201:8983/solr \
    --index gov2 \
    [--debug] \
    [&> solr-spark-output.log]
```

#### Experiment #3: HDFSSpark

```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode client \
    --name sent-detector-hdfs-spark \
    --class cs848.nlp.HDFSSpark \
    --conf spark.driver.memory=5g \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:cs848-nlp13 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
    --term <search-term> --field raw  \
    [&> hdfs-spark-output.log]
```

Count number of records:

```
cat <output-file>.log | grep "ID: " | wc -l
```

2. Core17:

```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode cluster \
    --name sent-detector \
    --class cs848.nlp.NLPDriver \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:spark-nlp4 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
    --solr --search <search-term> --field contents --collection http://tuna.cs.uwaterloo.ca:8983/solr/core17
```
