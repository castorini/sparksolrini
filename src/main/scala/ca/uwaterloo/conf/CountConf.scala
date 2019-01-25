package ca.uwaterloo.conf

import org.rogach.scallop.ScallopConf

class CountConf(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(input, output)

  val input = opt[String](descr = "input root path", required = true)
  val output = opt[String](descr = "output file path", required = true)

  verify()

}
