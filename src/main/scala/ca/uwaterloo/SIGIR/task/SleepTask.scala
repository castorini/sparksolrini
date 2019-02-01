package scala.ca.uwaterloo.SIGIR.task

class SleepTask(duration: Long) extends Task {
  def process(content: String) {
    Thread.sleep(duration)
  }
}
