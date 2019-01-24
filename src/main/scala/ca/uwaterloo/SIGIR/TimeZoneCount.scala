package ca.uwaterloo.SIGIR

import ca.uwaterloo.conf.SolrConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

object TimeZoneCount {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term) =
      (args.solr(), args.index(), args.rows(), args.field(), args.term())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .flatMap(doc => {
        val parsedJson = Json.parse(doc.get(field).toString)
        var timeZone:List[Tuple2[String, Int]] = List()
        try {
          val pair:Tuple2[String, Int] = ((parsedJson \ "user" \ "time_zone").as[String], 1)
          timeZone = List(pair)
        } catch {
          case e : Exception => {
            System.out.println("field time_zone unavailble for the following tweet")
            println(Json.prettyPrint(parsedJson))
          }
        }
        timeZone
      }).reduceByKey(_+_).sortBy(_._2, false)

    val topTimeZones = rdd.take(10)

    println(s"top 10 cities with the most tweets")
    topTimeZones.foreach(item => println(s"${item._1} --> ${item._2}"))

    log.info(s"Took ${System.currentTimeMillis - start}ms")
    sc.stop()
  }
}
