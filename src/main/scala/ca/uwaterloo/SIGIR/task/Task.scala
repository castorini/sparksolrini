package scala.ca.uwaterloo.SIGIR.task
import org.apache.log4j.Logger

abstract class Task(log: Logger) {
  def process(content:String)
}
