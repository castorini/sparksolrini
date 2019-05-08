#!/bin/bash

###
# Configuration
###

# The number of Spark executors
SPARK_NUM_EXECUTORS=8

# The number of cores per Spark executor
SPARK_EXECUTOR_CORES=1

# The amount of memory per Spark executor
SPARK_EXECUTOR_MEMORY=8G

# The amount of memory for the Spark driver
SPARK_DRIVER_MEMORY=4G

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip 0.0.0.0 --port 8181 --no-browser'

source $CONDA

# Activate conda env
conda activate sparksolrini

###
# Running
###

# Install (or update) Apache Toree kernel
jupyter toree install --user --spark_opts="--num-executors $SPARK_NUM_EXECUTORS --executor-cores $SPARK_EXECUTOR_CORES --executor-memory $SPARK_EXECUTOR_MEMORY --driver-memory $SPARK_DRIVER_MEMORY"

# Start the Jupyter server using the PySpark driver
pyspark -v --num-executors $SPARK_NUM_EXECUTORS --executor-cores $SPARK_EXECUTOR_CORES --executor-memory $SPARK_EXECUTOR_MEMORY --driver-memory $SPARK_DRIVER_MEMORY --jars spark-solr.jar
