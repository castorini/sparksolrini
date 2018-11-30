package ca.uwaterloo.cs848.util

import org.apache.lucene.analysis.CharArraySet
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

object Stemmer {

  val analyzer = new EnglishAnalyzer(CharArraySet.EMPTY_SET)

  def stem(text: String): String = {

    val tokenStream = analyzer.tokenStream(null, text)
    val builder = new StringBuilder

    tokenStream.reset

    while (tokenStream.incrementToken) {
      builder.append(tokenStream.getAttribute(classOf[CharTermAttribute]).toString).append(" ")
    }

    tokenStream.close

    builder.toString.trim

  }

}
