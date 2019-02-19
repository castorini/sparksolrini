package ca.uwaterloo.SIGIR

import ca.uwaterloo.Constants.MAX_ROW_PER_QUERY
import ca.uwaterloo.conf.TweetConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._


object KdeHour {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new TweetConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (term, field, solr, index) =
      (args.term(), args.field(), args.solr(), args.index())

    // Start timing the experiment
    val start = System.currentTimeMillis
    val timeRegex = raw"([0-9]+):([0-9]+):([0-9]+)".r

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(MAX_ROW_PER_QUERY)
      .query(field + ":" + term)
      .flatMap(doc => {
        val parsedJson = Json.parse(doc.get(field).toString)

        var out:List[Tuple3[Int, Double, Int]] = List()

        try {
          val timeZone:String = (parsedJson \ "user" \ "time_zone").as[String]
          if ((timeZone contains "Canada") || (timeZone contains "US")) {
            val time = (parsedJson \ "created_at").as[String]
            val matches = timeRegex.findFirstMatchIn(time)
            val hour = updateTime(matches.get.group(1).toInt, timeZone)
            val min = matches.get.group(2).toDouble
            out = List((hour, min/60, 1))
          }
        } catch {
          case e : Exception => {
              System.out.println("unable to parse the tweet", e)
          }
        }
        out
      }).persist()

    val counts = rdd.map(item => (item._1, item._3)).reduceByKey(_+_).sortByKey().collect().toMap

    val kdeData = rdd.map(item => item._1.toInt.toDouble + item._2)

    val kd = new KernelDensity().setSample(kdeData).setBandwidth(2.0)

    val domain = (0 to 23).toArray
    val densities = kd.estimate(domain.map(_.toDouble))

    println(s"counts / density per hour for $term")
    domain.foreach(x => {
      println(s"$x ( ${counts(x)} ) -- ${densities(x)}")
    })

    log.info(s"Took ${System.currentTimeMillis - start}ms")
    sc.stop()
  }

  def updateTime(hour:Int, timeZone:String):Int = {
    var adjusted = hour
    timeZone match {
      case "Pacific Time (US & Canada)" => adjusted = shiftHours(hour, -8)
      case "Eastern Time (US & Canada)" => adjusted = shiftHours(hour, -5)
      case "Central Time (US & Canada)" => adjusted = shiftHours(hour, -5)
      case "Mountain Time (US & Canada)" => adjusted = shiftHours(hour, -6)
      case "Atlantic Time (Canada)" => adjusted = shiftHours(hour, -4)
    }
    adjusted
  }

  def shiftHours(hour:Int, shift:Int):Int = {
    var adjusted = hour + shift
    if (adjusted >= 24) {
      adjusted %= 24
    } else if (adjusted < 0) {
      adjusted += 24
    }
    adjusted
  }

}