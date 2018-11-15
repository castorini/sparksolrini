# CS848-project

---

## Submitting Spark job

1) download spark from http://apache.forsale.plus/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz and unzip

2) add your jar to `spark-2.4.0-bin-hadoop2.7/examples/jars`

3) build docker image using `docker image build spark-2.4.0-bin-hadoop2.7/kubernetes/dockerfiles/spark/Dockerfile`

&nbsp;&nbsp;&nbsp;&nbsp; for test run you can pull my jar - `docker pull ljj7975/spark`

4) move to spark-2.4.0-bin-hadoop2.7 and exec following command to run sample code
```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode cluster \
    --name spark-pi \
    --class <class_name> \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<image_name> \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/<jar_name>.jar 10000
```

&nbsp;&nbsp;&nbsp;&nbsp; `192.168.152.202` can also be used

&nbsp;&nbsp;&nbsp;&nbsp; note that `/opt/spark/examples/jars/spark-examples_2.11-2.4.0.jar` is location of target jar in the docker image

5) Once driver status has changed to Completed, `kubectl logs <spark_driver_pod_id>` to see logs


Refer following links for detail
- https://spark.apache.org/docs/latest/running-on-kubernetes.html
- https://weidongzhou.wordpress.com/2018/04/29/running-spark-on-kubernetes/

---

## OpenNLP

Run with 

```
mvn clean package
spark-submit --class cs848.nlp.NLPDriver target/cs848-project-1.0-SNAPSHOT.jar --input sample_text.txt
```

1) Sentence Detection on Solr Docs

```
spark-submit --class cs848.nlp.NLPDriver target/cs848-project-1.0-SNAPSHOT.jar --solr --search "search-term"
```

2) Sentence Detection on Text Files

```
spark-submit --class cs848.nlp.NLPDriver target/cs848-project-1.0-SNAPSHOT.jar --input "file-path"
```
