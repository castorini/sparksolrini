package ca.uwaterloo.SIGIR

import ca.uwaterloo.Constants.{MILLIS_IN_DAY, MAX_ROW_PER_QUERY}
import ca.uwaterloo.conf.SolrConf
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.solr.common.params.CursorMarkParams
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Optional

import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj.SolrQuery.SortClause

import scala.ca.uwaterloo.SIGIR.task.{SentenceDetectionTask, SleepTask, Task}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object LocalFetch {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term, taskType, duration) =
      (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.task(), args.duration())

    // Start timing the experiment
    val start = System.currentTimeMillis

    log.info(s"\tStart Time of the experiment ${start} ms")

    val ids = List(1,2,3,4,5,6,7,8,9)

    val workers = sc.parallelize(ids, 9)

    log.info("\t Number of Partition : " + workers.getNumPartitions)

    val querySize = 1000

    val latency = workers.mapPartitions(iter => {

      log.info(s"\t partition ID : ${iter.next()}")

//      // Build the SolrClient.
//      val solrClient = new CloudSolrClient.Builder(solrList, Optional.of("/"))
//        .withConnectionTimeout(MILLIS_IN_DAY)
//        .build()
//
//      // Set the default collection
//      solrClient.setDefaultCollection(index)
//
//      // group doc ids into smaller group
//      val groupedDocIds = ListBuffer[ListBuffer[String]]()
//      var docIds = ListBuffer[String]()
//
//      while(iter.hasNext) {
//        if (docIds.size == querySize) {
//          groupedDocIds += docIds
//          docIds = ListBuffer[String]()
//        }
//        docIds += iter.next()
//      }
//      groupedDocIds += docIds
//
//      log.info(s"\t Number of grouped doc - " + groupedDocIds.size)
//
//      var queryTime:Long = 0
//      var processTime:Long = 0
//
//      var task:Task = null
//      log.info(s"Creating task: ${taskType}")
//
//      taskType match {
//        case "sleep" => task = new SleepTask(duration)
//        case "sd" => task = new SentenceDetectionTask()
//      }
//
//      groupedDocIds.zipWithIndex.foreach{ case(docIdValues, index) => {
//        // SolrJ cursor setup
//        var done = false
//        var cursorMark = CursorMarkParams.CURSOR_MARK_START
//
//        val docIdValuesStr = docIdValues.mkString(" OR ")
//
//        log.info(s"\n\tQuerying Solr for doc id group index ${index}")
//
//        // Create OR clause containing doc ids in the group
//        val query = new SolrQuery("id:( " + docIdValuesStr + ")").setSort(SortClause.asc("id"))
//        query.setRows(querySize) // same as querySize
//
//        while (!done) {
//          // Update cursor
//          query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark)
//
//          log.info(s"\tReading in next batch w/ cursorMark=${cursorMark}")
//
//          val queryStartTime = System.currentTimeMillis
//
//          // Do query
//          val response = solrClient.query(query, METHOD.POST)
//
//          queryTime += (System.currentTimeMillis - queryStartTime)
//
//          // Get new cursor from response
//          val nextCursorMark = response.getNextCursorMark
//
//          // The documents
//          val docs = response.getResults
//          log.info(s"\tNum docs retrieved: ${docs.size}")
//
//          val processStartTime = System.currentTimeMillis
//
//          // Do sentence detection in a new Thread
//          if (!docs.isEmpty) {
//            docs.asScala.foreach(doc => {
//              task.process(doc.get(field).toString)
//            })
//          }
//
//          processTime += (System.currentTimeMillis - processStartTime)
//
//          // End of results
//          if (cursorMark.equals(nextCursorMark)) {
//            done = true
//          }
//
//          // Update prev cursor
//          cursorMark = nextCursorMark
//        }
//      }}
//
//      // Clean-up
//      solrClient.close
//
//      log.info(s"\tQuery Time : ${queryTime} ms")
//      log.info(s"\tProcess Time : ${processTime} ms")
//
//      val timeList = List((queryTime, processTime))
//      timeList.iterator
      List((10, 10)).iterator
    }).reduce((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    })

    log.info(s"\tSum of Query Time : ${latency._1} ms")
    log.info(s"\tSum of Process Time : ${latency._2} ms")
    log.info(s"\tExperiment Time : ${System.currentTimeMillis - start} ms")
  }
}
