# CS848-project

Spark Docs
https://spark.apache.org/docs/2.3.2/running-on-kubernetes.html

Minikube w/ Spark
https://itnext.io/running-spark-job-on-kubernetes-minikube-958cadaddd55

---

## OpenNLP

Run with 

```
mvn clean package
spark-submit --class cs848.nlp.NLPDriver target/cs848-project-1.0-SNAPSHOT.jar --input sample_text.txt
```
