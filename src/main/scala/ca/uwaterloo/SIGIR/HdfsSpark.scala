package ca.uwaterloo.SIGIR

import ca.uwaterloo.conf.HdfsConf
import ca.uwaterloo.util.Stemmer
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

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
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<DOC>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</DOC>")

    val (path, debug, term, taskType) = (args.path(), args.debug(), args.term(), args.task())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val rdd = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
      .filter(doc => Stemmer.stem(doc._2.toString).contains(Stemmer.stem(term))) // Stemming to match Solr results
      .foreachPartition(part => {

      var task:Task = null
      log.info(s"\tCreating task : " + taskType)

      taskType match {
        case "sleep" => task = new SleepTask(log)
        case "sd" => task = new SentenceDetectionTask(log)
      }

      part.foreach(doc => {
        task.process(doc._2.toString)
    })

    log.info(s"Took ${System.currentTimeMillis - start}ms")

  }

}
