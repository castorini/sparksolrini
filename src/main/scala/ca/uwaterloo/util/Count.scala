package ca.uwaterloo.util

import java.nio.charset.StandardCharsets

import ca.uwaterloo.conf.CountConf
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
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

    val (inputDir, outputDir) = (args.input(), args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(outputDir), true)

    val rdd = sc.newAPIHadoopFile(inputDir, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])
      .filter(pair => {
        pair._2.header != null && pair._2.header.contentLengthStr != null && pair._2.header.contentTypeStr.equals("application/http;msgtype=response")
      })
      .map(pair => IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8)) // Get the HTML as a String
      .flatMap(line => line.split(" "))
      .map(word => (Stemmer.stem(word), 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 100)
      .sortBy(x => x._2)
      .coalesce(1, shuffle = true)
      .saveAsTextFile(outputDir)
  }
}
