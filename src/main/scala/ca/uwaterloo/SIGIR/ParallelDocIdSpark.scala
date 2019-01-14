package ca.uwaterloo.SIGIR

import ca.uwaterloo.cs848.Solr.MILLIS_IN_DAY
import ca.uwaterloo.cs848.conf.SolrConf
import ca.uwaterloo.cs848.util.SentenceDetector
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.solr.common.params.{CursorMarkParams, MapSolrParams, SolrParams}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Optional
import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj.SolrQuery.SortClause
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object ParallelDocIdSpark {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term, debug) = (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.debug())

    // Start timing the experiment
    val start = System.currentTimeMillis

    log.info(s"\tStart Time of the experiment ${start}ms")

    // Step 1 : Retrieve doc ids from Solr

    val solrList = new java.util.ArrayList[String]()
    solrList.add(solr)

    // Build the SolrClient.
    val solrClient = new CloudSolrClient.Builder(solrList, Optional.of("/"))
      .withConnectionTimeout(MILLIS_IN_DAY)
      .build()

    // Set the default collection
    solrClient.setDefaultCollection(index)

    log.info(s"\tfield : ${field}, term : ${term}")

    // Retrieve Doc Ids
    val query = new SolrQuery(field + ":" + term)
    query.setRows(rows)

    // make sure id is the correct field name
    query.addField("id")

    val docIdStart = System.currentTimeMillis

    // Do query
    val response = solrClient.query(query)

    val docIdStartTimeElasped = System.currentTimeMillis - docIdStart

    log.info(s"\tDocId Retreival Time : ${docIdStartTimeElasped}ms")


    // Step 2 : Parallelize Doc ids

    // Parallelize Doc ids
    val docs = response.getResults
    solrClient.close

    if (docs.isEmpty) {
      log.error("\tSearch Result is Empty")
      sys.exit(0)
    }

    val docIds = ListBuffer[String]()
    docs.asScala.foreach(doc => {
      docIds += doc.get("id").toString()
    })

    val distDocIds = sc.parallelize(docIds)

    log.info("\t Number of Docs : " + docIds.size + ", Number of Partition : " + distDocIds.getNumPartitions)

    // Step 3 : Retrieve individual partitions

    val latency = distDocIds.mapPartitions(iter => {

      // Build the SolrClient.
      val solrClient = new CloudSolrClient.Builder(solrList, Optional.of("/"))
        .withConnectionTimeout(MILLIS_IN_DAY)
        .build()

      // Set the default collection
      solrClient.setDefaultCollection(index)

      // SolrJ cursor setup
      var done = false
      var cursorMark = CursorMarkParams.CURSOR_MARK_START

      // Create OR clause containing every doc id in this partition
      var docIdValues = ListBuffer[String]()

      while(iter.hasNext) {
        docIdValues += iter.next()
      }

      val docIdValuesStr = docIdValues.mkString(" OR ")

      log.info("\tQuerying Solr for " + docIdValuesStr.size + " doc ids")

      val query = new SolrQuery("id:( " + docIdValuesStr + ")").setSort(SortClause.asc("id"))

      val sentenceDetector = new SentenceDetector()

      var queryTime:Long = 0
      var processTime:Long = 0

      while (!done) {
        // Update cursor
        query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark)

        log.info("\tQuerying Solr w/ cursorMark=${cursorMark}")

        val queryStartTime = System.currentTimeMillis

        // Do query
        val response = solrClient.query(query, METHOD.POST)

        queryTime += (System.currentTimeMillis - queryStartTime)

        // Get new cursor from response
        val nextCursorMark = response.getNextCursorMark

        // The documents
        val docs = response.getResults
        log.info("\tNum docs retrieved: ${docs.size}")

        val processStartTime = System.currentTimeMillis

        // Do sentence detection in a new Thread
        if (!docs.isEmpty) {
          docs.asScala.foreach(doc => {
            val sentences = sentenceDetector.inference(doc.get(field).toString)
            log.info("\tSentence Detection ran for doc : " + doc.get("id"))
          })
        }

        processTime += (System.currentTimeMillis - processStartTime)

        // End of results
        if (cursorMark.equals(nextCursorMark)) {
          done = true
        }

        // Update prev cursor
        cursorMark = nextCursorMark
      }

      // Clean-up
      solrClient.close

      log.info(s"\tQuery Time : ${queryTime}ms")
      log.info(s"\tProcess Time : ${processTime}ms")

      val timeList = List((queryTime, processTime))
      timeList.iterator
    }).reduce((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    })

    log.info(s"\tSum of Query Time : ${latency._1} ms")
    log.info(s"\tSum of Process Time : ${latency._2} ms")
    log.info(s"\tExperiment Time : ${System.currentTimeMillis - start} ms")
  }
}
