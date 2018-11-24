/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package cs848.wordcount

import collection.mutable.HashMap
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def wcIter(iter: Iterator[String]): Iterator[(String, Int)] = {
    val counts = new HashMap[String, Int]() { override def default(key: String) = 0 }

    iter.flatMap(line => tokenize(line))
      .foreach { t => counts.put(t, counts(t) + 1) }

    counts.iterator
  }

  def main(argv: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count").setJars(Array("/opt/spark/examples/jars/cs848-project-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs://192.168.152.203/test/20/0005447.xml")
    textFile
      .flatMap(line => tokenize(line))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect().foreach(println)
  }
}
