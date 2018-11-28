package cs848.nlp

import scala.collection.JavaConverters._
import org.apache.log4j.{Logger, PropertyConfigurator}
import cs848.util.{SentenceDetector, SolrQuery}

object SolrSeq {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("log4j.properties")

  def main(argv: Array[String]) = {

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
    val queryResult = SolrQuery.query(collectionUrls, searchField, searchTerm, index).asScala
    log.info("Num docs: " + queryResult.size)

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
