package ca.uwaterloo.util

import java.nio.charset.StandardCharsets

import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.jwat.warc.WarcRecord

object Count {
  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(args: Array[String]) = {

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val (collection, inputDir, outputFile) = (args(0), args(1), args(2))

    val rdd = sc.newAPIHadoopFile(inputDir, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])
      .filter(pair => {
        pair._2.header != null && pair._2.header.contentLengthStr != null && pair._2.header.contentTypeStr.equals("application/http;msgtype=response")
      })
      .map(pair => IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8)) // Get the HTML as a String // TODO only content?
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outputFile)
  }
}