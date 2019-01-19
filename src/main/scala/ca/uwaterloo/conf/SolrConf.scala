package ca.uwaterloo.conf

import org.rogach.scallop.ScallopConf

class SolrConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(term, field, solr, index, sleep)

  val field = opt[String](descr = "search field", required = true)
  val term = opt[String](descr = "search term", required = true)

  val solr = opt[String](descr = "Solr base URLs", required = true)
  val index = opt[String](descr = "Solr index name", default=Some("cw09b"), required = true)
  val task = opt[String](descr = "type of processing task to run", default = Some("sleep"))

  val rows = opt[Int](descr = "number of rows to return per request", default = Some(1000))
  val parallelism = opt[Int](descr = "number of cores/executors/etc. to use", default = Some(12))

  val sleep = opt[Boolean](descr = "sleep after each doc")


  codependent(field, term, solr, index)

  verify()

}
