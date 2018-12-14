# CS848-project

---

## Spark

### Submitting Spark job

1) Download spark from http://apache.forsale.plus/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz and unzip

2) Build jar by running `mvn clean package` under `CS848-project`

3) Add your jar to `spark-2.4.0-bin-hadoop2.7/examples/jars`

4) Build docker image of spark using `./bin/docker-image-tool.sh -r <docker_hub_id> -t <tag_name> build`

5) Push docker image of spark using `./bin/docker-image-tool.sh -r <docker_hub_id> -t <tag_name> push`

6) `ssh` into tem127

7) Download spark from http://apache.forsale.plus/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz and unzip on tembo

8) Run job with following command format
&nbsp;&nbsp;&nbsp;&nbsp; from tem127 (cluster mode)
```
bin/spark-submit \
    --master k8s://http://192.168.152.201:8080 \
    --deploy-mode cluster \
    --name <job_name> \
    --class <class_name> \
    --conf spark.driver.memory=5g \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<image_name>:<image_tag> \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/<jar_name>.jar <input>
```

&nbsp;&nbsp;&nbsp;&nbsp; Note that class_name, image_name, image_tag, jar_name must be correctly provided. `/opt/spark/examples/jars/<jar_name>.jar` is location of target jar in the docker image

&nbsp;&nbsp;&nbsp;&nbsp; `192.168.152.202` can also be used as the master

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
python3 collect_metrics.py <seq/solr/spark> <search term> <sleep time in sec>
```
the script will create two .txt files under `metrics` directory, one for nodes, one for pods.
each file will have start timestamp as part of its name.

Similarly, to collect resource usage of driver running on tem127,
```
python3 collect_driver_metrics.py <seq/solr/spark> <search term> <sleep time in sec>
```
