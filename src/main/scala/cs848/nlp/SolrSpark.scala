package cs848.nlp

import scala.collection.JavaConverters._
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import cs848.util.{SentenceDetector, SolrQuery}

class SolrConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(search, field, collection, index, debug)
  val search = opt[String](descr = "search term", required=true)
  val field = opt[String](descr = "search field", required=true)
  val collection = opt[String](descr = "collection url", required=true)
  val index = opt[String](descr = "index name", required=true)
  val debug = opt[Boolean](descr = "debug / print")

  codependent(search, field, collection, index)

  verify()
}

object SolrSpark {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("log4j.properties")

  def main(argv: Array[String]) = {

    log.info("abc123 1")
    val conf = new SparkConf().setAppName("Solr Spark Driver")
    log.info("abc123 2")
    val sc = new SparkContext(conf)
    log.info("abc123 3")

    val args = new SolrConf(argv)

    val searchTerm = args.search().toString
    log.info("Search Term: " + searchTerm)

    val searchField = args.field().toString
    log.info("Search Field: " + searchField)

    val collectionUrls = args.collection()
    log.info("Collection URL: " + collectionUrls)

    val index = args.index()
    log.info("Index Name: " + index)

    val debug = args.debug()
    log.info("Debug: " + debug)

    // query Solr
    log.info("Querying Solr")
    log.info("abc123 4")
    val queryResult = SolrQuery.queryRDD(searchField, searchTerm, index, sc)
    log.info("abc123 5")
    log.info("Num docs: " + queryResult.count)

    // sentence detection
    log.info("Performing sentence detection")
    val docs = queryResult.map(doc => {
      log.info("abc123 6")
      val sents = SentenceDetector.inference(doc.get(searchField).toString, searchField)
      log.info("abc123 7")
      if (debug) {
        log.info("ID: " + doc.get("id"))
        sents.foreach(println)
      }
    })
  }
}
