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

    val conf = new SparkConf().setAppName("Solr Spark Driver") //.setJars(Array("/opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)

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
    val queryResult = SolrQuery.queryRDD(searchField, searchTerm, index, sc)
    log.info("Num docs: " + queryResult.count)

    // sentence detection
    log.info("Performing sentence detection")
    val docs = queryResult.map(doc => {
      val sents = SentenceDetector.inference(doc.get(searchField).toString, searchField)
      if (debug) {
        log.info("ID: " + doc.get("id"))
        sents.foreach(println)
      }
    })
  }
}
