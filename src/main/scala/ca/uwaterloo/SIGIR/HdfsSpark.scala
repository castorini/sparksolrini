package ca.uwaterloo.SIGIR

import java.nio.charset.StandardCharsets

import ca.uwaterloo.conf.HdfsConf
import ca.uwaterloo.util.Stemmer
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.jwat.warc.WarcRecord

import scala.ca.uwaterloo.SIGIR.task.{SentenceDetectionTask, SleepTask, Task}

object HdfsSpark {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    val args = new HdfsConf(argv)
    log.info(args.summary)

    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val (path, term, taskType, duration, filter) = (args.path(), args.term(), args.task(), args.duration(), args.filter())

    // Start timing the experiment
    val start = System.currentTimeMillis

    var rdd = sc.newAPIHadoopFile(path, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])

    // Do we filter?
    if (filter) {
      rdd = rdd.filter(pair => {
        pair._2.header != null && pair._2.header.contentLengthStr != null && pair._2.header.contentTypeStr.equals("application/http;msgtype=response")
      })
    }

    rdd
      .map(pair => IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8)) // Get the HTML as a String
      .filter(doc => Stemmer.stem(doc).contains(Stemmer.stem(term))) // Stemming to match Solr results
      .foreachPartition(part => {

      var task: Task = null
      log.info(s"Creating task: ${taskType}")

      taskType match {
        case "sleep" => task = new SleepTask(duration)
        case "sd" => task = new SentenceDetectionTask()
      }

      part.foreach(doc => {
        task.process(doc)
      })

    })

    log.info(s"Took ${System.currentTimeMillis - start}ms")

  }

}