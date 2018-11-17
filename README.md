# CS848-project

Spark Docs
https://spark.apache.org/docs/2.3.2/running-on-kubernetes.html

Minikube w/ Spark
https://itnext.io/running-spark-job-on-kubernetes-minikube-958cadaddd55

---

## OpenNLP

### Cluster:

Run ```sudo docker pull zeynepakkalyoncu/spark:spark-nlp-latest``` to get the latest image

1. Sentence Detection on ClueWeb:

```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode cluster \
    --name sent-detector \
    --class cs848.nlp.NLPDriver \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:spark-nlp-latest \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
    --solr --search <search-term> --field raw --collection http://192.168.152.201:30852/solr/cw09b
```

2. Core17:

```
bin/spark-submit \
    --master k8s://https://192.168.152.201:6443 \
    --deploy-mode cluster \
    --name sent-detector \
    --class cs848.nlp.NLPDriver \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=zeynepakkalyoncu/spark:spark-nlp-latest \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar \
    --solr --search <search-term> --field contents --collection http://tuna.cs.uwaterloo.ca:8983/solr/core17
```

### Single Node:

Build: 

```
mvn clean package
```

Run:

1) Sentence Detection on Solr Docs

```
spark-submit --class cs848.nlp.NLPDriver target/cs848-project-1.0-SNAPSHOT.jar --solr --search <search-term> --field <field>
```

2) Sentence Detection on Text Files

```
spark-submit --class cs848.nlp.NLPDriver target/cs848-project-1.0-SNAPSHOT.jar --input <file-path>
```
