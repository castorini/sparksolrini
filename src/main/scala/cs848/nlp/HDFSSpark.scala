package cs848.nlp

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.jsoup.Jsoup
import cs848.util.{SentenceDetector, Stemmer}

class HDFSConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(term, field, debug)

  val term = opt[String](descr = "search term", required=true)
  val field = opt[String](descr = "search field", required=true)
  val path = opt[String](descr = "hdfs path", required=true)
  val debug = opt[Boolean](descr = "debug / print")

  codependent(term, field)

  verify()
}

object HDFSSpark {

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) = {

    val conf = new SparkConf().setAppName("HDFS Spark Driver").setJars(Array("/opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val args = new HDFSConf(argv)

    val searchTerm = Stemmer.stem(args.term())
    log.info("Search Term: " + searchTerm)

    val searchField = args.field().toString
    log.info("Search Field: " + searchField)

    val hdfsPath = args.path()
    log.info("HDFS Path: " + hdfsPath)

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

    val sentenceDetector = new SentenceDetector()

    val docs = sc.wholeTextFiles("hdfs://192.168.152.203" + hdfsPath + "/*") // TODO: potential OOM
      .filter(_._2.contains(searchTerm))
      .map(_._2.toString)

    // sentence detection
    log.info("Performing sentence detection")
    docs.map(doc => {
      val sents = sentenceDetector.inference(doc, searchField)
      if (debug) {
        println("ID: ")
        sents.foreach(println)
      }
      sents
    })
  }
}
