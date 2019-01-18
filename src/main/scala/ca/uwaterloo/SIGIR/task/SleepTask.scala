package scala.ca.uwaterloo.SIGIR.task

import org.apache.log4j.Logger

class SleepTask(log:Logger) extends Task(log) {
  log.info(s"\tSleep Task Constructed")

  def process(content:String): Unit = {
    Thread.sleep(50)
  }
}
