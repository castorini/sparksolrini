package ca.uwaterloo.cs848


import ca.uwaterloo.cs848.Solr.MILLIS_IN_DAY
import ca.uwaterloo.cs848.conf.SolrConf
import ca.uwaterloo.cs848.util.SentenceDetector
import com.google.common.base.Splitter
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.solr.common.params.CursorMarkParams
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._


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

    // Step 1 : Retrieve doc ids from Solr

    // Parse Solr URLs
    val solrUrls = Splitter.on(',').splitToList(args.solr())

    // Build the SolrClient.
    val solrClient = new CloudSolrClient.Builder(solrUrls)
      .withConnectionTimeout(MILLIS_IN_DAY)
      .build()

    // Set the default collection
    solrClient.setDefaultCollection(args.index())

    // Retrieve Doc Ids
    val query = new SolrQuery(args.field() + ":" + args.term())
    query.setRows(args.rows())

    // make sure id is the correct field name
    query.addField("id")

    // Do query
    val response = solrClient.query(query)

    // Step 2 : Parallelize Doc ids

    // Parallelize Doc ids
    val docs = response.getResults
    log.info(s"Num docs: ${docs.size}")

    if (docs.isEmpty) {
      log.error("Search Result is Empty")
      sc.stop()

      sys.exit(0)
    }

    val docIds = List[String]()
    docs.asScala.foreach(doc => {
      docIds ++ doc.get("id").toString()
    })

    val distDocIds = sc.parallelize(docIds)

    // Step 3 : Retrieve individual partitions

    distDocIds.foreachPartition(iter => {

      // Parse Solr URLs
      val solrUrls = Splitter.on(',').splitToList(args.solr())

      // Build the SolrClient.
      val solrClient = new CloudSolrClient.Builder(solrUrls)
        .withConnectionTimeout(MILLIS_IN_DAY)
        .build()


      // SolrJ cursor setup
      var done = false
      var cursorMark = CursorMarkParams.CURSOR_MARK_START

      // Create OR clause containing every doc id in this partition
      var docIdValues = List[String]()

      while (iter.hasNext) {
        docIdValues ++ iter.next()
      }

      val docIdValuesStr = docIdValues.mkString(" OR ")

      log.info(s"Querying Solr for doc ids = $docIdValuesStr")

      val query = new SolrQuery("id:( " + docIdValuesStr + ")")

      val sentenceDetector = new SentenceDetector()

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

        // Do sentence detection in a new Thread
        if (!docs.isEmpty) {

          log.info("Sentence detection starting...")

          docs.asScala.foreach(doc => {
            val sentences = sentenceDetector.inference(doc.get(args.field()).toString)
            if (args.debug()) {
              log.info("ID: " + doc.get("id"))
              sentences.foreach(println)
            }
          })
        }

        // End of results
        if (cursorMark.equals(nextCursorMark)) {
          done = true
        }

        // Update prev cursor
        cursorMark = nextCursorMark

      }

      // Clean-up
      solrClient.close
    })

    log.info(s"Took ${System.currentTimeMillis - start}ms")

    // Need to manually call stop()
    sc.stop()

  }
}
