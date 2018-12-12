package ca.uwaterloo.cs848

import ca.uwaterloo.cs848.conf.SolrConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

object SolrSpark {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term, debug) = (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.debug())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .foreachPartition(part => {
        var counter = 0
        part.foreach(_ => counter += 1)
        log.info(s"$counter docs in partition")
      })

    log.info(s"Took ${System.currentTimeMillis - start}ms")

    // Need to manually call stop()
    sc.stop()

  }
}
