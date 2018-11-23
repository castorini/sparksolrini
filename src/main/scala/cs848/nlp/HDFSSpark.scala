package cs848.nlp

import javax.security.auth.login.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.rogach.scallop.ScallopConf
import org.jsoup.Jsoup
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import de.l3s.archivespark._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.specific.warc._
import de.l3s.archivespark.specific.warc.specs._

class HDFSSparkConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(search, field)

  val search = opt[String](descr = "search term")
  val field = opt[String](descr = "search field")

//  val path = opt[String](descr = "hdfs path")

  codependent(search, field)

  verify()
}

object HDFSSpark {

  val log = Logger.getLogger(getClass.getName)

  // load NLP model
  val modelIn = getClass.getClassLoader.getResourceAsStream("en-sent-detector.bin")

  // set up NLP model
  val model = new SentenceModel(modelIn)
  val sentDetector = new SentenceDetectorME(model)

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("HDFS Spark Driver")
    val sc = new SparkContext(conf)

    val args = new HDFSSparkConf(argv)

    val searchTerm = args.search()
    log.info("Search Term: " + searchTerm)

    val searchField = args.field()
    log.info("Search Field: " + searchField)

    val warc = ArchiveSpark.load(WarcHdfsSpec("hdfs://192.168.152.203/ClueWeb09b/ClueWeb09_English_1/*/*.warc"))

    val docs = warc
      .enrich(HtmlText)
//      .peekJson
      .filterValue(HtmlText) (v => v.get.contains(searchTerm))

    println("Original:")
    println(docs)

    println("########")
    println("Filtered and split:")
    inference(docs)
      .foreach(println)
  }

  def inference(inputText : String) = { sentDetector.sentDetect(inputText) }
}
