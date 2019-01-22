package ca.uwaterloo.SIGIR

import ca.uwaterloo.conf.SolrConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import scala.ca.uwaterloo.SIGIR.task.{SentenceDetectionTask, SleepTask, Task}

object SolrRddSpark {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term, taskType) =
      (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.task())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .foreachPartition(partition => {
        
        var task:Task = null
        log.info(s"\tCreating task : " + taskType)

        taskType match {
          case "sleep" => task = new SleepTask(log)
          case "sd" => task = new SentenceDetectionTask(log)
        }

        partition.foreach(doc => {
          task.process(doc.get(field).toString)
        })
      })

    log.info(s"Took ${System.currentTimeMillis - start}ms")

    // Need to manually call stop()
    sc.stop()

  }
}
