package ca.uwaterloo.SIGIR

import scala.collection.JavaConverters._
import java.net.URL

import com.google.common.net.InternetDomainName
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.jsoup.Jsoup

import scala.ca.uwaterloo.conf.WebConf


object LinkAnalysis {
  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new WebConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val (solr, index, field, term, rows, parallelism, output) = (args.solr(), args.index(), args.field(), args.term(), args.rows(), args.parallelism(), args.output())

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val source_urls = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .repartition(parallelism)
      .mapPartitions(docs => {
        docs.map(doc => {
          val url = doc.get("url") + ""
          (InternetDomainName.from(new URL(url.substring(1, url.length - 1)).getHost).topPrivateDomain().name(), doc.get("raw") + "")
        })
      })


    val zipped_urls = source_urls
      .sample(false, 0.01, 42)
      .flatMap(record => {
        val target_urls = Jsoup.parse(record._2)
          .select("a[href]")
          .asScala
          .map(link => link.attr("abs:href"))
          .filter(!_.isEmpty)
          .map(link => {
            try { InternetDomainName.from(new URL(link).getHost).topPrivateDomain().name() }
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
      .coalesce(1)
      .saveAsTextFile(output)

    sc.stop()

  }
}
