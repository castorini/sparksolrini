package ca.uwaterloo.util

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

object HdfsDist {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val file = sc.textFile("hdfs://node-master:9000/term-count/part-*")
	
    val dist = file
	.map(line => {
		val perc = line.substring(1, line.length - 1).splitAt(line.lastIndexOf(','))._2
		(math.floor(perc.toDouble) * 50220.403, 1)
	})
        .reduceByKey(_ + _)
	.map(x => (math.log(x._2), math.log(x._1)))
	.sortByKey()
	.coalesce(1, false)
	
        dist.saveAsTextFile("hdfs://node-master:9000/cw09b_dist")
  }
}
