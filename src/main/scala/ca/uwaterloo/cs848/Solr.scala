package ca.uwaterloo.cs848

import java.util.concurrent.{Executors, TimeUnit}

import ca.uwaterloo.cs848.conf.SolrConf
import ca.uwaterloo.cs848.util.SentenceDetector
import com.google.common.base.Splitter
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.SortClause
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrDocumentList
import org.apache.solr.common.params.CursorMarkParams

import scala.collection.JavaConverters._

object Solr {

  val MILLIS_IN_DAY = 1000 * 60 * 60 * 24

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Parse Solr URLs
    val solrUrls = Splitter.on(',').splitToList(args.solr())

    // Build the SolrClient.
    val solrClient = new CloudSolrClient.Builder(solrUrls)
      .withConnectionTimeout(MILLIS_IN_DAY)
      .build()

    // Set the default collection
    solrClient.setDefaultCollection(args.index())

    // # of executors = # of cores
    val executorService = Executors.newFixedThreadPool(args.parallelism())

    // SolrJ cursor setup
    var done = false
    var cursorMark = CursorMarkParams.CURSOR_MARK_START

    // The query to run
    val query = new SolrQuery(args.field() + ":" + args.term()).setRows(args.rows()).setSort(SortClause.asc("id"))

    // Start timing the experiment now
    val start = System.currentTimeMillis

    while (!done) {

      // Update cursor
      query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark)

      log.info(s"Querying Solr w/ cursorMark=$cursorMark")

      // Do query
      val response = solrClient.query(query)

      // Get new cursor from response
      val nextCursorMark = response.getNextCursorMark

      // The documents
      val docs = response.getResults
      log.info(s"Num docs: ${docs.size}")

//      // Do sentence detection in a new Thread
//      if (!docs.isEmpty) {
//        executorService.submit(new SentenceDetectionTask(docs, args.field(), args.debug()))
//      }

      // End of results
      if (cursorMark.equals(nextCursorMark)) {
        done = true
      }

      // Update prev cursor
      cursorMark = nextCursorMark

    }

    // Clean-up
    solrClient.close
    executorService.shutdown

    // Wait for any remaining sentence detection tasks to finish
    while (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
      log.info("Waiting for sentence detection to finish...")
    }

    // Print out total time
    log.info(s"Took ${System.currentTimeMillis - start}ms")
    printf(s"Took ${System.currentTimeMillis - start}ms")
  }

  // Wrap a Runnable so we can pass in some params
  class SentenceDetectionTask(docs: SolrDocumentList, searchField: String, debug: Boolean) extends Runnable {

    val sentenceDetector = new SentenceDetector()

    override def run(): Unit = {

      log.info("Sentence detection starting...")

      docs.asScala.foreach(doc => {
        val sentences = sentenceDetector.inference(doc.get(searchField).toString)
        if (debug) {
          log.info("ID: " + doc.get("id"))
          sentences.foreach(println)
        }
      })

      log.info("Sentence detection done...")

    }
  }

}
