package ca.uwaterloo.cs848.conf

import org.rogach.scallop.ScallopConf

class SolrConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(term, field, solr, index, debug)

  val field = opt[String](descr = "search field", required = true)
  val term = opt[String](descr = "search term", required = true)

  val solr = opt[String](descr = "Solr base URLs", required = true)
  val index = opt[String](descr = "Solr index name", required = true)

  val rows = opt[Int](descr = "number of rows to return per request", default = Some(1000))
  val debug = opt[Boolean](descr = "debug / print")

  codependent(term, field, solr, index)

  verify()

}