package scala.ca.uwaterloo.SIGIR.task
import org.apache.log4j.Logger

class ClusteringTask(log:Logger) extends Task(log) {
  log.info(s"\tClustering Task Constructed")

  def process(content:String): Unit = {
    System.out.println(content)
  }
}
