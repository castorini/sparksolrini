package ca.uwaterloo.SIGIR

import ca.uwaterloo.conf.SolrConf
import ca.uwaterloo.util.SentenceDetector
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

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

    val (solr, index, rows, field, term, sleep) = (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.sleep())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .foreachPartition(partition => {
           if (sleep) { partition.foreach(doc => Thread.sleep(100)) }
           else {
               val sentenceDetector = new SentenceDetector()
               partition.foreach(doc => sentenceDetector.inference(doc.get(field).toString))
           } 
      })

    log.info(s"Took ${System.currentTimeMillis - start}ms")

    // Need to manually call stop()
    sc.stop()

  }
}
