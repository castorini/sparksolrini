package ca.uwaterloo.cs848.util

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import org.jsoup.Jsoup

// Common to each SentenceDetector instance
object SentenceDetector {

  val model = new SentenceModel(getClass.getClassLoader.getResourceAsStream("en-sent-detector.bin"))

  def parse(inputText: String): String = {
    // parse HTML document
    val htmlDoc = Jsoup.parse(inputText)
    try {
      htmlDoc.body().text()
    } catch {
      case e: Exception => println("exception caught: " + e);
        ""
    }
  }

}

// SentenceDetectorME isn't thread-safe, need a new Object per Thread.
class SentenceDetector {

  val sentenceDetector = new SentenceDetectorME(SentenceDetector.model)

  def inference(inputText: String, searchField: String) = {
    val input = if (searchField.equals("raw")) SentenceDetector.parse(inputText) else inputText
    sentenceDetector.sentDetect(input)
  }

}
