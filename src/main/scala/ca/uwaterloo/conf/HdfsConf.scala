package ca.uwaterloo.conf

import scala.ca.uwaterloo.conf.Conf

class HdfsConf(args: Seq[String]) extends Conf(args) {

  mainOptions = Seq(term, path)

  val path = opt[String](descr = "hdfs path", required = true)

  verify()

}