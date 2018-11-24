package cs848.util

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

object SentenceDetector {

  val modelStream = getClass.getClassLoader.getResourceAsStream("en-sent-detector.bin")
  val model = new SentenceModel(modelStream)

  val sentDetector = new SentenceDetectorME(model)

  def inference(inputText: String) = {
    sentDetector.sentDetect(inputText)
  }

}
