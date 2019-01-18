package scala.ca.uwaterloo.SIGIR.task

import ca.uwaterloo.util.SentenceDetector
import org.apache.log4j.Logger

class SentenceDetectionTask(log:Logger) extends Task(log) {
  log.info(s"\tSentence Detection Task Constructed")

  val sentenceDetector = new SentenceDetector()

  def process(content:String): Unit = {
      val sentences = sentenceDetector.inference(content)
  }
}
