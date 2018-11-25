package cs848.nlp

import com.github.takezoe.solr.scala._
import cs848.util.SentenceDetector
import org.apache.log4j.Logger
import org.jsoup.Jsoup
import org.rogach.scallop.ScallopConf

class SolrSeqConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(search, field, collection)
  val search = opt[String](descr = "search term")
  val field = opt[String](descr = "search field")
  val collection = opt[String](descr = "collection url")

  codependent(search, field, collection)

  verify()
}

object SolrSeq {

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) = {

    val args = new SolrSeqConf(argv)

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
      .getResultAsMap(Map(searchField -> searchTerm.toString))

    val docs = queryResult.documents.map(doc => {
      val docMap = scala.collection.mutable.Map[String, String]()

      docMap("id") = doc("id").toString

      if (searchField.equals("raw")) {
        // parse HTML document
        val htmlDoc = Jsoup.parse(doc(searchField).toString)
        docMap(searchField) = htmlDoc.body().text()
      }
      else {
        docMap(searchField) = doc(searchField).toString
      }

      docMap

    })

    println("Original:")
    docs
      .foreach { doc =>
        println("id: " + doc("id"))
        println(searchField + ": " + doc(searchField))
      }

    // TODO: check if this runs as a sequential process
    println("########")
    println("Filtered and split:")
    docs
      .foreach { doc =>
        println("id: " + doc("id"))
        SentenceDetector.inference(doc(searchField))
          .foreach(println)
      }
  }

}