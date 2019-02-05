package ca.uwaterloo.SIGIR

import java.nio.charset.StandardCharsets
import java.net.URL
import org.jsoup.Jsoup

import ca.uwaterloo.conf.HdfsConf
import ca.uwaterloo.util.Stemmer
import nl.surfsara.warcutils.WarcInputFormat
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.jwat.warc.WarcRecord

object LinkAnalysis {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    val args = new HdfsConf(argv)
    log.info(args.summary)

    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val (path, term, taskType, duration) = (args.path(), args.term(), args.task(), args.duration())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val source_urls = sc.newAPIHadoopFile(path, classOf[WarcInputFormat], classOf[LongWritable], classOf[WarcRecord])
      .filter(pair => {
        pair._2.header != null &&
          pair._2.header.contentLengthStr != null &&
          pair._2.header.contentTypeStr.equals("application/http;msgtype=response") &&
        Stemmer.stem(IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8)).contains(Stemmer.stem(term))
      })
      .flatMap(pair => {
        val content = Stemmer.stem(IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8))
        val pair_list = List[(String, String)]()
        val links = Jsoup.parse(content).select("a[href]").attr("href")
          for (link <- links) {
            pair_list :+ (new URL(pair._2.header.warcTargetUriStr).getHost, link)
          }
        pair_list
    })
  }
}