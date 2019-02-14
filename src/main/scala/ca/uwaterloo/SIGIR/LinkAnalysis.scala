package ca.uwaterloo.SIGIR

import scala.collection.JavaConverters._

import java.nio.charset.StandardCharsets
import java.net.URL
import org.jsoup.Jsoup

import ca.uwaterloo.conf.HdfsConf
import ca.uwaterloo.util.Stemmer
import com.google.common.net.InternetDomainName
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
        pair._2.header != null && pair._2.header.contentLengthStr != null && pair._2.header.contentTypeStr.equals("application/http;msgtype=response")
      })
      .map(pair => {
        try {
          (InternetDomainName.from(new URL(pair._2.header.warcTargetUriStr).getHost.toString).topPrivateDomain().name(), IOUtils.toString(pair._2.getPayloadContent, StandardCharsets.UTF_8))
        }
        catch {
          case e: Exception => println(e)
            ("", "")
        }
      })
      .filter(doc => !doc._1.isEmpty && Stemmer.stem(doc._2).contains(Stemmer.stem(term)))

    val zipped_urls = source_urls
      .sample(false, 0.01, 42)
      .flatMap(record => {
        val target_urls = Jsoup.parse(record._2)
          .select("a[href]")
          .asScala
          .map(link => link.attr("abs:href"))
          .filter(!_.isEmpty)
          .map(link => {
            try {
              InternetDomainName.from(new URL(link).getHost).topPrivateDomain().name()
            }
            catch {
              case e: Exception => println(e)
                ""
            }
          })
          .distinct
          .take(3)

        val src_host = (1 to target_urls.size).map(_ => record._1)
        src_host zip target_urls
      })
      .distinct
      .filter(x => x._1 != x._2)
      .map(pair => pair._1 + ";" + pair._2)
      .repartition(1)
      .saveAsTextFile("hdfs://node-master:9000/links")
  }
}
