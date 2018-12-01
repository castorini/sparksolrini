package ca.uwaterloo.cs848.conf

import org.rogach.scallop.ScallopConf

class HdfsConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(term, field, debug)

  val term = opt[String](descr = "search term", required = true)
  val field = opt[String](descr = "search field", required = true)
  val path = opt[String](descr = "hdfs path", required = true)
  val debug = opt[Boolean](descr = "debug / print")

  codependent(term, field)

  verify()

}