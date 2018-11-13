package cs848.nlp

import scala.io.Source

import java.io.FileInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.rogach.scallop.ScallopConf

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

import com.github.takezoe.solr.scala._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(solr, search, input, multiPath, train)
  val solr = opt[Boolean](descr = "do solr query", required = false)

  val search = opt[String](descr = "search term", required = false) // solr query
  val input = opt[String](descr = "input path", required = false) // read file

  val multiPath = opt[Boolean](descr = "multi path", required = false)
  val train = opt[Boolean](descr = "train", required = false)
  verify()
}

object NLPDriver {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("Sentence Detector Driver")
    val sc = new SparkContext(conf)

    val args = new Conf(argv)

    val solr = args.solr()
    val multiPath = args.multiPath()
    val train = args.train()

    log.info("Solr: " + solr)
    log.info("Multi-path: " + multiPath) // TODO
    log.info("Train: " + train) // TODO

    if (solr) {
      // do solr query
      val client = new SolrClient("http://tuna.cs.uwaterloo.ca:8983/solr/core17")

      // query Solr
      val searchTerm = args.search()
      log.info("Search Term: " + searchTerm)

      val queryResult = client.query("contents: %contents%")
        .fields("id", "contents")
        .sortBy("id", Order.asc)
        .getResultAsMap(Map("contents" -> searchTerm.toString))

      val docs = queryResult.documents

      docs
        .foreach { doc: Map[String, Any] =>
          println("id: " + doc("id"))
          println("contents: " + doc("contents"))
        }

      if (train) {
        // TODO: train?
      }
      else {
        // inference only
        docs
          .foreach { doc: Map[String, Any] =>
            inference(doc("contents").toString)
            println()
          }
      }
    }
    else {
      val inputPath = args.input()
      log.info("Input Path: " + inputPath)

      // read from file
      val bufferedSource = Source.fromFile(inputPath)
      val docs = bufferedSource.getLines().mkString

      println(docs)

      if (train) {
        // TODO: train?
      }
      else {
        // inference only
        inference(docs)
      }

      bufferedSource.close
    }
  }

  def inference(inputText : String) = {
    // load model
    val modelIn = new FileInputStream("models/en-sent-detector.bin")

    // set up model
    val model = new SentenceModel(modelIn)
    val sentDetector = new SentenceDetectorME(model)

    // run model
    val result = sentDetector.sentDetect(inputText)
      .foreach(println)
  }
}
