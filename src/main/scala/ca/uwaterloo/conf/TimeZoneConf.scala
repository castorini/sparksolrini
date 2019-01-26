package ca.uwaterloo.conf

import org.rogach.scallop.ScallopConf

class TimeZoneConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(term1, term2, num, field, solr, index, parallelism)

  val field = opt[String](descr = "search field", required = true)
  val term1 = opt[String](descr = "first term to search", required = true)
  val term2 = opt[String](descr = "second term to search", required = true)
  val num = opt[Int](descr = "number of timezones to compare", required = true)

  val solr = opt[String](descr = "Solr base URLs", required = true)
  val index = opt[String](descr = "Solr index name", default=Some("cw09b"), required = true)
  val parallelism = opt[Int](descr = "number of cores/executors/etc. to use", default = Some(12))

  codependent(term1, term2, num, field, solr, index)

  verify()

}
