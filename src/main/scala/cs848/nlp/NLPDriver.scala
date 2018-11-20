package cs848.nlp

import scala.io.Source

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.rogach.scallop.ScallopConf

import org.jsoup.Jsoup

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

import com.github.takezoe.solr.scala._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(solr, search, field, collection, input)
  val solr = opt[Boolean](descr = "do solr query")

  val search = opt[String](descr = "search term")
  val field = opt[String](descr = "search field")
  val collection = opt[String](descr = "collection url")

  val input = opt[String](descr = "input file path")

  conflicts(solr, List(input))
  codependent(solr, search, field, collection)

  verify()
}

object NLPDriver {

  val log = Logger.getLogger(getClass.getName)

  // load NLP model
  val modelIn = getClass.getClassLoader.getResourceAsStream("en-sent-detector.bin")

  // set up NLP model
  val model = new SentenceModel(modelIn)
  val sentDetector = new SentenceDetectorME(model)

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("Sentence Detector Driver")
    val sc = new SparkContext(conf)

    val args = new Conf(argv)

    val solr = args.solr()

    log.info("Solr: " + solr)

    if (solr) {
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

      println("########")
      println("Filtered and split:")
      docs
        .foreach { doc =>
          println("id: " + doc("id"))
          inference(doc(searchField))
            .foreach(println)
        }
    }
    else {
      val inputPath = args.input()
      log.info("Input Path: " + inputPath)

      // read from file
      val bufferedSource = Source.fromFile(inputPath)
      val docs = bufferedSource.getLines().mkString

      println("Original:")
      println(docs)

      println("########")
      println("Filtered and split:")
      inference(docs)
        .foreach(println)

      bufferedSource.close
    }
  }

  def inference(inputText : String) = { sentDetector.sentDetect(inputText) }
}
