package cs848.nlp

import scala.io.Source

import java.io.FileInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.rogach.scallop.ScallopConf

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

import com.github.takezoe.solr.scala._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(solr, search, input)
  val solr = opt[Boolean](descr = "do solr query", required = false)

  val search = opt[String](descr = "search term", required = false) // search term
  val input = opt[String](descr = "input path", required = false) // input file path

  verify()
}

object NLPDriver {

  val log = Logger.getLogger(getClass().getName())

  // load NLP model
  val modelIn = new FileInputStream("models/en-sent-detector.bin")

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
      val client = new SolrClient("http://tuna.cs.uwaterloo.ca:8983/solr/core17")
//      val client = new SolrClient("http://192.168.152.201:30852/solr/cw09b")

      // query Solr
      val searchTerm = args.search()
      log.info("Search Term: " + searchTerm)

      val queryResult = client.query("contents: %contents%")
        .fields("id", "contents")
        .sortBy("id", Order.asc)
        .getResultAsMap(Map("contents" -> searchTerm.toString))

      val docs = queryResult.documents

      println("Original:")
      docs
        .foreach { doc: Map[String, Any] =>
          println("id: " + doc("id"))
          println("contents: " + doc("contents"))
        }

      println("########")
      println("Filtered and split:")
      docs
        .foreach { doc: Map[String, Any] =>
          println("id: " + doc("id"))
          inference(doc("contents").toString)
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
