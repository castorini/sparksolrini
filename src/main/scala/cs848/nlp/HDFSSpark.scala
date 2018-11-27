//package cs848.nlp

package hdfs.jsr203

import cs848.util.{SentenceDetector, Stemmer}

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import java.nio.file.{Files, Paths}
import java.net.URI

import org.jsoup.Jsoup

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

    val conf = new SparkConf().setAppName("HDFS Spark Driver").setJars(Array("/opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val args = new HDFSSparkConf(argv)

    val searchTerm = Stemmer.stem(args.search())
    log.info("Search Term: " + searchTerm)

    val searchField = args.field()
    log.info("Search Field: " + searchField)

    // traverse files recursively
//    val hdfs = FileSystem.get(new URI("hdfs://192.168.152.203"), sc.hadoopConfiguration)
//    val paths = hdfs.listFiles(new Path("/ClueWeb09b/ClueWeb09_English_1"), true)
//
//    var docs = Map[String, String]()
//    while (paths.hasNext) {
//      val path = paths.next().getPath().toString
//      val textFile = sc.textFile(path).toString
//
//      docs += ("id" -> textFile)
//    }

    val docs = sc.wholeTextFiles("hdfs://192.168.152.203/ClueWeb09b/ClueWeb09_English_1/*/*") // TODO: potential OOM
      .filter(_._2.contains(searchTerm))
      .map(_._2.toString)
//      .map(pair => ("id" -> pair(1))) // (fileName, fileContent)

    // sentence detection
    docs
//      .map(doc => SentenceDetector.inference(doc, searchField))
      .foreach(doc => {
        println("ID: ")
        SentenceDetector.inference(doc, searchField)
          .foreach(println)
      })
  }
}
