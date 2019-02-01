package ca.uwaterloo.SIGIR

import java.util.Properties

import ca.uwaterloo.conf.SolrConf
import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConversions._
import scala.ca.uwaterloo.conf.NerConf

object NamedEntityRecognition {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new NerConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term, entityType, parallelism, debug) = (args.solr(), args.index(), args.rows(), args.field(), args.term(), args.entity(), args.parallelism(), args.debug())

    val output = args.output()
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val tokens = sc.longAccumulator("tokens")
    val time = sc.longAccumulator("time")

    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .repartition(parallelism)
      .mapPartitions(docs => {

        val props = new Properties()
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")
        props.setProperty("ner.applyFineGrained", "false")
        props.setProperty("ner.useSUTime", "false")

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

          var entities = coreDoc.entityMentions()

          if (!entityType.equals("*"))
            entities = entities.filter(cem => cem.entityType().equals(entityType))

          entities

        })

        // Processing time for partition
        time.add(System.currentTimeMillis() - start)

        entities

      })
      .saveAsTextFile(output)



    sc.stop()

  }

}