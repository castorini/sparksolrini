package cs848.nlp

import com.github.takezoe.solr.scala._
import cs848.util.SentenceDetector
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import org.rogach.scallop.ScallopConf

class SolrSparkConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(search, field, collection)
  val search = opt[String](descr = "search term")
  val field = opt[String](descr = "search field")
  val collection = opt[String](descr = "collection url")

  codependent(search, field, collection)

  verify()
}

object SolrSpark {

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("Solr Spark Driver").setJars(Array("/opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)

    val args = new SolrSparkConf(argv)

    // do solr query
    val searchTerm = args.search()
    log.info("Search Term: " + searchTerm)

    val searchField = args.field()
    log.info("Search Field: " + searchField)

    val collectionUrl = args.collection()
    log.info("Collection URL: " + collectionUrl)

    val client = new SolrClient(collectionUrl)

    // query Solr
    val queryResult = client.query(searchField + ": %" + searchField + "%")
      .fields("id", searchField)
      .sortBy("id", Order.asc)
      .rows(50000)
      .getResultAsMap(Map(searchField -> searchTerm.toString))

    // sentence detection
    val docs = queryResult.documents
//      .map(doc => SentenceDetector.inference(doc(searchField).toString, searchField))
        .foreach(doc => {
          println("ID: " + doc("id"))
          SentenceDetector.inference(doc(searchField).toString, searchField)
          .foreach(println)
        })
  }
}
