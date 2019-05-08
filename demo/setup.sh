#!/bin/bash

###
# Configuration
###

# The spark-solr version to download
SPARK_SOLR_VERSION=3.6.0

###
# Setup
###

# Source conda script
source $CONDA

# Create and activate the env
conda create -n sparksolrini python=3.7 && conda activate sparksolrini

# Install dependencies
pip install -U jupyter pyspark toree

# Download spark-solr
wget "https://search.maven.org/remotecontent?filepath=com/lucidworks/spark/spark-solr/$SPARK_SOLR_VERSION/spark-solr-$SPARK_SOLR_VERSION-shaded.jar" -O spark-solr.jar
