package scala.ca.uwaterloo.SIGIR.task

import ca.uwaterloo.util.SentenceDetector

class SentenceDetectionTask extends Task {
  val sentenceDetector = new SentenceDetector()

  def process(content: String): Array[String] = {
    sentenceDetector.inference(content)
  }
}