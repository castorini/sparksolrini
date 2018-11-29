package cs848.util

import scala.collection.JavaConversions._
import java.util
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.params.MapSolrParams
import com.lucidworks.spark.rdd._
import org.apache.spark.SparkContext

object SolrQuery {

  def query(collectionUrls: String, searchField: String, searchTerm: String, index: String) = {
    val solrUrls = collectionUrls.split(",").toSeq
    val solrClient = new CloudSolrClient.Builder().withSolrUrl(solrUrls).build()

    solrClient.setConnectionTimeout(86400000)

    val queryParamMap = new util.HashMap[String, String]()
    queryParamMap.put("q", searchField + ":" + searchTerm)
    queryParamMap.put("rows", 2147483630.toString)
    val queryParams = new MapSolrParams(queryParamMap)

    val response = solrClient.query(index, queryParams)
    response.getResults
  }

  def queryRDD(searchField: String, searchTerm: String, index: String, sc: SparkContext) = {
    val solr = new SelectSolrRDD("192.168.152.201:32181", index, sc)
    val result = solr.query(searchField + ":" + searchTerm)
    result
  }
}
