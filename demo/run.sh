#!/bin/bash

###
# Configuration
###

# The number of Spark executors
SPARK_NUM_EXECUTORS=1

# The number of cores per Spark executor
SPARK_EXECUTOR_CORES=8

# The amount of memory per Spark executor
SPARK_EXECUTOR_MEMORY=8G

# The amount of memory for the Spark driver
SPARK_DRIVER_MEMORY=4G

source ${CONDA}

# Activate conda env
conda activate sparksolrini

###
# Running
###

jupyter notebook --ip 0.0.0.0 --port 8181 --no-browser