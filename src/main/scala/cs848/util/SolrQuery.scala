package cs848.util

import com.lucidworks.spark.rdd._
import org.apache.spark.SparkContext

object SolrQuery {

  def queryRDD(searchField: String, searchTerm: String, index: String, sc: SparkContext) = {
    new SelectSolrRDD("192.168.152.201:32181", index, sc).query(searchField + ":" + searchTerm)
  }

}
