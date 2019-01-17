package ca.uwaterloo.conf

import org.rogach.scallop.ScallopConf

class HdfsConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(term, path)

  val term = opt[String](descr = "search term", required = true)
  val path = opt[String](descr = "hdfs path", required = true)
  val debug = opt[Boolean](descr = "debug / print")

  verify()

}
