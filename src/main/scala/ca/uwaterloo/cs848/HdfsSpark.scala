package ca.uwaterloo.cs848

import ca.uwaterloo.cs848.conf.HdfsConf
import ca.uwaterloo.cs848.util.{SentenceDetector, Stemmer}
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

object HdfsSpark {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("log4j.properties")

  def main(argv: Array[String]) = {

    val args = new HdfsConf(argv)
    log.info(args.summary)

    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<DOC>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</DOC>")

    val (debug, term) = (args.debug(), args.term())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val rdd = sc.newAPIHadoopFile(args.path(), classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
      .filter(doc => Stemmer.stem(doc._2.toString).contains(Stemmer.stem(term))) // Stemming to match Solr results
      .count()

    log.info(s"Took ${System.currentTimeMillis - start}ms")

  }

}