package scala.ca.uwaterloo.conf

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {

  val term = opt[String](descr = "search term", required = true)

  val task = opt[String](descr = "type of processing task to run", default = Some("sleep"))
  val duration = opt[Long](descr = "if using sleep task, how many ms to sleep for", default = Some(5))

  val debug = opt[Boolean](descr = "debug / print")

}