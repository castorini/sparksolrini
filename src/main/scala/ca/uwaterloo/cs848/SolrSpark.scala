package ca.uwaterloo.cs848

import ca.uwaterloo.cs848.conf.SolrConf
import ca.uwaterloo.cs848.util.SentenceDetector
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.spark.{SparkConf, SparkContext}

object SolrSpark {

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) = {

    // Configure logging, no need for log4j.properties
    BasicConfigurator.configure()

    // Set debugging log level
    Logger.getRootLogger.setLevel(Level.DEBUG)

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term, debug) = (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.debug())

    // Start timing the experiment
    val start = System.currentTimeMillis

    // The query to run
    val query = new SolrQuery(field + ":" + term).setSort(SortClause.asc("id"))

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(query)
      .foreachPartition(partition => {
        val sentenceDetector = new SentenceDetector()
        partition.foreach(doc => {
          val sentences = sentenceDetector.inference(doc.get(field).toString)
          if (debug) {
            log.info("ID: " + doc.get("id"))
            sentences.foreach(println)
          }
        })

      })


    log.info(s"Took ${System.currentTimeMillis - start}ms")

    // Need to manually call stop()
    sc.stop()

  }
}
