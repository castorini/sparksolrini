package ca.uwaterloo.util

import java.nio.charset.StandardCharsets

import ca.uwaterloo.conf.CountConf
import ca.uwaterloo.util.Stemmer
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.jwat.warc.WarcRecord

object Count {
  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    val args = new CountConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val inputDir = "hdfs://node-master:9000/collections/cw09b"
    val outputFile = "count.txt"

//    val inputDir = args.input()
//    val outputFile = args.output()
//    val (inputDir, outputFile) = (args(0), args(1))

    log.info(inputDir)
    log.info(outputFile)

    val rdd = sc.newAPIHadoopFile(inputDir, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])
      .filter(pair => {
        pair._2.header != null && pair._2.header.contentLengthStr != null && pair._2.header.contentTypeStr.equals("application/http;msgtype=response")
      })
      .map(pair => IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8)) // Get the HTML as a String
      .flatMap(line => line.split(" "))
      .map(word => (Stemmer.stem(word), 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2)
      .saveAsTextFile(outputFile)
  }
}
