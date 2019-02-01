package ca.uwaterloo.SIGIR

import java.util.Properties

import ca.uwaterloo.conf.SolrConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

object NamedEntityRecognition {

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

    val output = "entities"
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val tokens = sc.longAccumulator("tokens")
    val time = sc.longAccumulator("time")

    val rdd = new SelectSolrRDD(solr, index, sc, maxRows = Some(10))
      .rows(rows)
      .query(field + ":" + term)
      .mapPartitions(docs => {

        val props = new Properties()
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")

        val pipeline = new StanfordCoreNLP(props)

        val start = System.currentTimeMillis()

        val entities = docs.map(doc => {

          if (debug) {
            log.info(s"########\n# ${doc.get("id")}\n########\n")
          }

          val text = doc.get(field).toString

          // Create the doc and annotate it
          val coreDoc = new CoreDocument(text)
          pipeline.annotate(coreDoc)

          // Keep track of # of tokens
          tokens.add(coreDoc.tokens().size())

          coreDoc.entityMentions()

        })

        // Processing time for partition
        time.add(System.currentTimeMillis() - start)

        entities

      })
      .saveAsTextFile(output)

    val duration = time.value
    val rate = tokens.value / (duration / 1000)

    log.info(s"Took ${duration}ms @ ${rate} token/s")

    sc.stop()

  }

}