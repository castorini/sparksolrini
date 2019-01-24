package ca.uwaterloo.SIGIR

import ca.uwaterloo.Constants.MAX_ROW_PER_QUERY
import ca.uwaterloo.conf.TimeZoneConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable.ListBuffer

object TimeZone {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new TimeZoneConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (term1, term2, num, field, solr, index) =
      (args.term1(), args.term2(), args.num(), args.field(), args.solr(), args.index())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val timeZones1 = getTopTimeZones(sc, solr, index, field, term1, num)
    val timeZones2 = getTopTimeZones(sc, solr, index, field, term2, num)

    println(s"$term1 - top $num cities with the most tweets")
    timeZones1.foreach(pair => println(s"\t${pair._1} --> ${pair._2}"))

    println(s"$term2 - top $num cities with the most tweets")
    timeZones2.foreach(pair => println(s"\t${pair._1} --> ${pair._2}"))

    val timeZones = ListBuffer[String]()
    timeZones1.foreach(pair => timeZones += pair._1)

    var count = 0
    timeZones2.foreach(pair => {
      if (timeZones.contains(pair._1)) {
        count += 1
      }
    })

    println(s"$term1 and $term2 - $count / $num (${Math.round(count*100/num)} %)")

    log.info(s"Took ${System.currentTimeMillis - start}ms")
    sc.stop()
  }

  def getTopTimeZones(sc:SparkContext, solr:String, index:String, field:String, term:String, n:Int = 10) = {
    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(MAX_ROW_PER_QUERY)
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

    rdd.take(n)
  }
}
