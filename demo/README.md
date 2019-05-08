# Setup

Make sure [conda](https://conda.io/en/latest/) is installed and export the following variable (making the appropriate substition for your install location):

`export CONDA=/opt/miniconda3/etc/profile.d/conda.sh`

Also, ensure that the `SPARK_HOME` environment variable is set to the location of your Spark install.

# Running

```
./setup.sh # Do the setup (conda environment, pip dependencies, spark-solr, etc.)
./run.sh # Run the notebook server
```

The output from the run script will have the URL for the notebook server with Spark support (both PySpark and Scala).
