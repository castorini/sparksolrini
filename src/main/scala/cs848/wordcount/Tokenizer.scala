package cs848.wordcount

import java.util.StringTokenizer

import scala.collection.JavaConverters._

trait Tokenizer {
  def tokenize(s: String): List[String] = {
    val pattern = """(^[^a-z]+|[^a-z]+$)""".r

    new StringTokenizer(s).asScala.toList
      .map(t => pattern.replaceAllIn(t.asInstanceOf[String].toLowerCase(), ""))
      .filter(_.length != 0)
  }
}
