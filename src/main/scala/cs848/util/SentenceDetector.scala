package cs848.util

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import org.jsoup.Jsoup

object SentenceDetector {

  val modelStream = getClass.getClassLoader.getResourceAsStream("en-sent-detector.bin")
  val model = new SentenceModel(modelStream)

  val sentDetector = new SentenceDetectorME(model)

  def parse(inputText: String): String = {
    // parse HTML document
    val htmlDoc = Jsoup.parse(inputText)
    htmlDoc.body().text()
  }

  def inference(inputText: String, searchField: String) = {
    val input = if (searchField.equals("raw")) parse(inputText) else inputText
    sentDetector.sentDetect(input)
  }
}
