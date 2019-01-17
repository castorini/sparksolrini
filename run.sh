#!/bin/bash

export PATH="$PATH:/localdisk5/hadoop/hadoop/bin:/localdisk5/hadoop/hadoop/sbin"
export PATH="$PATH:/localdisk5/hadoop/spark/bin"

export JAVA_HOME=/usr/lib/jvm/java-8-oracle/jre
export HADOOP_CONF_DIR=/localdisk5/hadoop/hadoop/etc/hadoop
export SPARK_HOME=/localdisk5/hadoop/spark
export LD_LIBRARY_PATH=/localdisk5/hadoop/hadoop/lib/native:$LD_LIBRARY_PATH

: '
spark-submit \
    --name sent-detector-parallel-spark \
    --class ca.uwaterloo.SIGIR.ParallelDocIdSpark \
    --num-executors 9 --executor-cores 8 --executor-memory 48G --driver-memory 32G \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term $1 \
    --field raw \
    --solr 192.168.1.111:9983 \
    --index gov2
'

spark-submit \
    --name sent-detector-hdfs-spark \
    --class ca.uwaterloo.SIGIR.HdfsSpark \
    --num-executors 9 --executor-cores 1 --executor-memory 48G --driver-memory 32G \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term $1 \
    --path /collections/gov2

: '
spark-submit \
    --name sent-detector-solr-spark \
    --class ca.uwaterloo.SIGIR.SolrRddSpark \
    --num-executors 9 --executor-cores 8 --executor-memory 48G --driver-memory 32G \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --field raw \
    --term $1 \
    --row 1000 \
    --solr 192.168.1.111:9983 \
    --index gov2
'
