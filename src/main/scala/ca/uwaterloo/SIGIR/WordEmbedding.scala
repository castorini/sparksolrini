package ca.uwaterloo.SIGIR

import ca.uwaterloo.conf.SolrConf
import ca.uwaterloo.util.Stemmer
import com.lucidworks.spark.rdd.SelectSolrRDD
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

object WordEmbedding {

  val log = Logger.getLogger(getClass.getName)
  PropertyConfigurator.configure("/localdisk0/etc/log4j.properties")

  def main(argv: Array[String]) = {

    // Parse command line args
    val args = new SolrConf(argv)
    log.info(args.summary)

    // Setup Spark
    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val (solr, index, rows, field, term) =
      (args.solr(), args.index(), args.rows(), args.field(), args.term())

    // Start timing the experiment
    val start = System.currentTimeMillis

    val word2vec = new Word2Vec()
    val rdd = new SelectSolrRDD(solr, index, sc)
      .rows(rows)
      .query(field + ":" + term)
      .map(doc => {
        val parsedJson = Json.parse(doc.get(field).toString)
        val contents = Stemmer.stem(parsedJson("text").toString)
        contents.toString.split(" ").toSeq
      })

    val model = word2vec.fit(rdd)

    val synonyms = model.findSynonyms(term, 10)

    println(s"top 10 similar words")
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym ---- $cosineSimilarity")
    }

    println(s"vector mappings")
    val vectors = model.getVectors
    vectors foreach ( (t2) => println (t2._1 + "-->" + t2._2.mkString(" ")))

    log.info(s"Took ${System.currentTimeMillis - start}ms")

    // Need to manually call stop()
    sc.stop()
  }
}
