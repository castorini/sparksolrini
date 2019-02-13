package ca.uwaterloo.SIGIR

import java.util.Properties

import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.ca.uwaterloo.conf.NerConf
import scala.collection.JavaConversions._

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

    val (solr, index, rows, searchField, contentField, term, entityType, parallelism, debug) = (args.solr(), args.index(), args.rows(), args.searchField(), args.contentField(), args.term(), args.entity(), args.parallelism(), args.debug())

    val output = args.output()
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val props = sc.broadcast(new Properties() {{
      setProperty("annotators", "tokenize,ssplit,pos,lemma,ner")
      setProperty("ner.applyFineGrained", "false")
      setProperty("ner.useSUTime", "false")
    }})

    new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(searchField + ":" + term)
      .repartition(parallelism)
      .mapPartitions(docs => {
        val pipeline = new StanfordCoreNLP(props.value)
        docs.map(doc => {

          log.debug(s"########\n# ${doc.get("id")}\n########\n")

          val coreDoc = new CoreDocument(doc.get(contentField).toString)
          pipeline.annotate(coreDoc)

          entityType match {
            case "*" => coreDoc.entityMentions()
            case _ => coreDoc.entityMentions().filter(_.entityType().equals(entityType))
          }
        })
      })
      .saveAsTextFile(output)

    sc.stop()

  }

}
