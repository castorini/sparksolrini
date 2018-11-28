package cs848.nlp

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.jsoup.Jsoup
import cs848.util.{SentenceDetector, Stemmer}

class HDFSConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(search, field, debug)

  val search = opt[String](descr = "search term")
  val field = opt[String](descr = "search field")
  val debug = opt[Boolean](descr = "debug / print")

  codependent(search, field)

  verify()
}

object HDFSSpark {

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("HDFS Spark Driver").setJars(Array("/opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val args = new HDFSConf(argv)

    val searchTerm = Stemmer.stem(args.search())
    log.info("Search Term: " + searchTerm)

    val searchField = args.field()
    log.info("Search Field: " + searchField)

    val debug = args.debug()
    log.info("Debug: " + debug)

    // read from HDFS
    log.info("Reading from HDFS")
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

    // sentence detection
    log.info("Performing sentence detection")
    docs.foreach(doc => {
      val sents = SentenceDetector.inference(doc, searchField)
      if (debug) {
        println("ID: ")
        sents.foreach(println)
      }
    })
  }
}
