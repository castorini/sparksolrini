package cs848.nlp

import scala.io.Source

import java.io.FileInputStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, multiPath, train)
  val input = opt[String](descr = "input path", required = true)
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

    val inputPath = args.input()
    val multiPath = args.multiPath()
    val train = args.train()

    log.info("Input Path: " + inputPath)
    log.info("Multi-path: " + multiPath)
    log.info("Train: " + train)

    val bufferedSource = Source.fromFile(inputPath)
    val inputText = bufferedSource.getLines().mkString
//      .foreach(println)

    if (train) {
      // TODO: train?
    }
    else {
      // inference only
      inference(inputText)
    }

    bufferedSource.close
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
