package cs848.nlp

import cs848.util.{SentenceDetector, Stemmer}
import de.l3s.archivespark._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.specs._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

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

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("HDFS Spark Driver")
    val sc = new SparkContext(conf)

    val args = new HDFSSparkConf(argv)

    val searchTerm = args.search()
    log.info("Search Term: " + searchTerm)

    val searchField = Stemmer.stem(args.field())
    log.info("Search Field: " + searchField)

    val warc = ArchiveSpark.load(WarcHdfsSpec("hdfs://192.168.152.203/ClueWeb09b/ClueWeb09_English_1/*/*.warc"))

    val docs = warc
      .enrich(HtmlText)
      //      .peekJson
      .filterValue(HtmlText)(v => Stemmer.stem(v.get).contains(searchTerm))
      .map(value => value.toString)

//    println("Original:")
//    println(docs)
//
//    println("########")
//    println("Filtered and split:")
//    SentenceDetector.inference(docs)
//      .foreach(println)
  }

}
