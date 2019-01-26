package ca.uwaterloo.SIGIR

import java.util.Properties

import ca.uwaterloo.conf.SolrConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

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

    val tokens = sc.longAccumulator("tokens")
    val time = sc.longAccumulator("time")

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .foreachPartition(part => {

        val props = new Properties()
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")

        val pipeline = new StanfordCoreNLP(props)

        val start = System.currentTimeMillis()

        part.foreach(doc => {

          if (debug) {
            log.info(s"########\n# ${doc.get("id")}\n########\n")
          }

          // The text
          val text = doc.get(field).toString

          // Create a new CoreDocument and annotate it using our pipeline
          val coreDoc = new CoreDocument(text)
          pipeline.annotate(coreDoc)

          // For each sentence,
          coreDoc.sentences().asScala.foreach(sent => {
            tokens.add(sent.tokens().size()) // Keep track of # of tokens
            if (debug) {
              log.info(sent.text())
              log.info(sent.entityMentions().asScala.map(mention => (mention.text(), mention.entityType(), mention.charOffsets())).mkString("-> ", ",", "\n"))
            }
          })

        })

        time.add(System.currentTimeMillis() - start)

      })

    log.info(s"Took ${time.value}ms @ ${(tokens.value / (time.value / 1000))} token/s")

    sc.stop()

  }

}