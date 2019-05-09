# Information Retrieval Meets Scalable Text Analytics: Solr Integration with Spark

This repo contains the source code and demonstration notebooks for the SIGIR 2019 Demo "Information Retrieval Meets Scalable Text Analytics: Solr Integration with Spark".

---

## Setup - Maven

`HdfsSpark` requires [warcutils](https://github.com/norvigaward/warcutils) to work, but unfortunately it's not available in Maven Central (and their repo appears to be down). We need to build it ourself and install it to our local Maven repo.

```bash
git clone https://github.com/norvigaward/warcutils.git
cd warcutils
mvn clean package
mvn install:install-file -Dfile=target/warcutils-1.2.jar -DpomFile=pom.xml
```

## Spark

### Submitting Spark Job (Cluster Mode)

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
### Submitting Spark Job (Client Mode)

To use client mode, jar must be distributed by driver using `setJars` with location of jars inside docker image as following

```
val conf = new SparkConf()
  .setAppName(<app_name>)
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

To collect CPU and RAM usage on Kubernetes run following command on tem101.
```
python3 collect_metrics.py <seq/solr/spark> <search term> <sleep time in sec>
```
the script will create two .txt files under `metrics` directory, one for nodes, one for pods.
each file will have start timestamp as part of its name.

Similarly, to collect resource usage of driver running on tem127,
```
python3 collect_driver_metrics.py <seq/solr/spark> <search term> <sleep time in sec>
```

## Useful links

- Installing Kubernetes via Kubespray: https://github.com/kubernetes-sigs/kubespray
- HDFS on Kubernetes: https://github.com/apache-spark-on-k8s/kubernetes-HDFS

--

## Running on Himrod


```./run.sh <cw09b|gov2> <search-term> <<sleep|sd>```

---

- Add necessary destinations to your path each time you start a session
```
export PATH="$PATH:/localdisk5/hadoop/hadoop/bin:/localdisk5/hadoop/hadoop/sbin"
export PATH="$PATH:/localdisk5/hadoop/spark/bin"

export JAVA_HOME=/usr/lib/jvm/java-8-oracle/jre
export HADOOP_CONF_DIR=/localdisk5/hadoop/hadoop/etc/hadoop
export SPARK_HOME=/localdisk5/hadoop/spark
export LD_LIBRARY_PATH=/localdisk5/hadoop/hadoop/lib/native:$LD_LIBRARY_PATH
```

collections : `cb09b`

- ParallelDocIdSpark
```
spark-submit \
    --deploy-mode client \
    --name ParallelDocIdSpark \
    --class ca.uwaterloo.SIGIR.ParallelDocIdSpark \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term <term> \
    --field raw \
    --solr 192.168.1.111:9983 \
    --index <collection> \
    --task <sleep|sd>
```

- HdfsSpark
```
spark-submit \
    --deploy-mode client \
    --name HdfsSpark \
    --class ca.uwaterloo.SIGIR.HdfsSpark \
   --conf spark.executor.instances=10 \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term <term> \
    --path /collections/<collection> \
    --task <sleep|sd>
```

- SolrRddSpark
```
spark-submit \
    --deploy-mode client \
    --name SolrRddSpark \
    --class ca.uwaterloo.SIGIR.SolrRddSpark \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term <term> \
    --field raw \
    --solr 192.168.1.111:9983 \
    --index <collection> \
    --task <sleep|sd>
```

- WordEmbedding
```
spark-submit \
    --deploy-mode client \
    --name WordEmbedding \
    --class ca.uwaterloo.SIGIR.WordEmbedding \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term <term> \
    --field raw \
    --solr 192.168.1.111:9983 \
    --index mb11
```

- KdeDay/KdeHour
``` 
spark-submit \
    --deploy-mode client \
    --name <KdeHour|KdeDay> \
    --class ca.uwaterloo.SIGIR.<KdeHour|KdeDay> \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term <term> \
    --num <num> \
    --field raw \
    --solr 192.168.1.111:9983 \
    --index mb13
```

- Tweet
``` 
spark-submit \
    --deploy-mode client \
    --name Tweet \
    --class ca.uwaterloo.SIGIR.Tweet \
    target/cs848-project-1.0-SNAPSHOT.jar \
    --term <term> \
    --num <num> \
    --field raw \
    --solr 192.168.1.111:9983 \
    --index mb13
```

## Sample Tweet
```
{
  "created_at" : "Thu Jan 20 01:57:46 +0000 2011",
  "id" : 27907607423361024,
  "id_str" : "27907607423361024",
  "text" : "Justin was like \"I'mma tell you one time...baby baby baby Oh\" and Selena was all \"Tell me something I don't know!\"",
  "source" : "web",
  "truncated" : false,
  "user" : {
    "id" : 93289740,
    "id_str" : "93289740",
    "name" : "Zoey",
    "screen_name" : "Zoeybaby09",
    "location" : "Degrassi land 3",
    "url" : "http://degrassiheavenxo.tumblr.com/",
    "description" : " My life= One tree hill, Degrassi, the outsiders, wicked and rent! 99% unicorn and 1% Cat! ",
    "protected" : false,
    "followers_count" : 482,
    "friends_count" : 290,
    "listed_count" : 2,
    "created_at" : "Sun Nov 29 00:24:13 +0000 2009",
    "favourites_count" : 169,
    "geo_enabled" : false,
    "verified" : false,
    "statuses_count" : 25098,
    "lang" : "en",
    "contributors_enabled" : false,
    "is_translator" : false,
    "profile_background_color" : "642D8B",
    "profile_background_image_url" : "http://a0.twimg.com/profile_background_images/200143527/cork-board.gif",
    "profile_background_image_url_https" : "https://si0.twimg.com/profile_background_images/200143527/cork-board.gif",
    "profile_background_tile" : true,
    "profile_image_url" : "http://a0.twimg.com/profile_images/2400140258/qzjh2kduh0hoygarp7hu_normal.jpeg",
    "profile_image_url_https" : "https://si0.twimg.com/profile_images/2400140258/qzjh2kduh0hoygarp7hu_normal.jpeg",
    "profile_link_color" : "FF0000",
    "profile_sidebar_border_color" : "65B0DA",
    "profile_sidebar_fill_color" : "7AC3EE",
    "profile_text_color" : "3D1957",
    "profile_use_background_image" : true,
    "default_profile" : false,
    "default_profile_image" : false
  },
  "retweet_count" : 2,
  "entities" : {
    "hashtags" : [ ],
    "urls" : [ ],
    "user_mentions" : [ ]
  },
  "favorited" : false,
  "retweeted" : false,
  "requested_id" : 31716724621447168
}
```

- Count
```
nohup ./run.sh hdfs://node-master:9000/collections/cw09b term-count &
```
