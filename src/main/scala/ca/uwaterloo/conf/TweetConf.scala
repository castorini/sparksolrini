package ca.uwaterloo.conf

import org.rogach.scallop.ScallopConf

class TweetConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(term, num, field, solr, index, parallelism)

  val field = opt[String](descr = "search field", required = true)
  val term = opt[String](descr = "search term", required = true)
  val num = opt[Int](descr = "number of timezones to compare", required = true)
  val solr = opt[String](descr = "Solr base URLs", required = true)
  val index = opt[String](descr = "Solr index name", default=Some("cw09b"), required = true)
  val parallelism = opt[Int](descr = "number of cores/executors/etc. to use", default = Some(12))

  codependent(term,  num, field, solr, index)

  verify()

}
