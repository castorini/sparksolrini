package scala.ca.uwaterloo.SIGIR.task

import ca.uwaterloo.Constants.MILLIS_IN_MINUTE

class SleepTask(duration: Long) extends Task {
  def process(content: String) {
    Thread.sleep(duration * MILLIS_IN_MINUTE)
  }
}