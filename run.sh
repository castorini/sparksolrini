#!/bin/bash

export PATH="$PATH:/localdisk5/hadoop/hadoop/bin:/localdisk5/hadoop/hadoop/sbin"
export PATH="$PATH:/localdisk5/hadoop/spark/bin"

export JAVA_HOME=/usr/lib/jvm/java-8-oracle/jre
export HADOOP_CONF_DIR=/localdisk5/hadoop/hadoop/etc/hadoop
export SPARK_HOME=/localdisk5/hadoop/spark
export LD_LIBRARY_PATH=/localdisk5/hadoop/hadoop/lib/native:$LD_LIBRARY_PATH

# Term list
declare -a terms=("idea" "good" "intern" "event" "start" "end")

# Sleep duration list
duration=(3)

for d in "${duration[@]}"
do
    for t in "${terms[@]}"
    do
        # Task 1
        spark-submit \
        --deploy-mode client \
        --name "parallel-docid-spark-${t}-${d}"  \
        --class ca.uwaterloo.SIGIR.ParallelDocIdSpark \
        --conf spark.network.timeout=10000001s \
        --conf spark.executor.heartbeatInterval=10000000s \
        --conf spark.rpc.message.maxSize=500 \
        --num-executors 9 --executor-cores 16 --executor-memory 48G --driver-memory 32G \
        target/cs848-project-1.0-SNAPSHOT.jar \
        --term ${t} \
        --field raw \
        --solr 192.168.1.111:9983 \
        --index $1 \
        --task $2 \
        --duration ${d} \
        &> "parallel-docid-spark-${t}-${d}.txt"

        sleep 5m
     
       # Task 2
       spark-submit \
       --deploy-mode client \
       --name "hdfs-spark-${t}-${d}" \
       --class ca.uwaterloo.SIGIR.HdfsSpark \
       --conf spark.network.timeout=10000001s \
       --conf spark.executor.heartbeatInterval=10000000s \
       --conf spark.rpc.message.maxSize=500 \
       --num-executors 9 --executor-cores 16 --executor-memory 48G --driver-memory 32G \
       target/cs848-project-1.0-SNAPSHOT.jar \
       --term ${t} \
       --path "/collections/$1" \
       --task $2 \
       --duration ${d} \
       &> "hdfs-spark-${t}-${d}.txt"

       sleep 5m

       # Task 3
       spark-submit \
       --deploy-mode client \
       --name "solr-rdd-spark-${t}-${d}" \
       --conf spark.network.timeout=10000001s \
       --conf spark.executor.heartbeatInterval=10000000s \
       --conf spark.rpc.message.maxSize=500 \
       --class ca.uwaterloo.SIGIR.SolrRddSpark \
       --num-executors 9 --executor-cores 16 --executor-memory 48G --driver-memory 32G \
       target/cs848-project-1.0-SNAPSHOT.jar \
       --field raw \
       --term ${t} \
       --rows 1000 \
       --solr 192.168.1.111:9983 \
       --index $1 \
       --task $2 \
       --duration ${d} \
       &> "solr-rdd-spark-${t}-${d}.txt"

       done
done

: '
spark-submit \
    --name term-count \
    --class ca.uwaterloo.util.Count \
    --num-executors 9 --executor-cores 16 --executor-memory 48G --driver-memory 32G \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --input $1 \
    --output $2
'
