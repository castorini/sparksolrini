package cs848.util

import scala.collection.JavaConversions._
import java.util
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.params.MapSolrParams

object SolrQuery {

  def query(collectionUrls: String, searchField: String, searchTerm: String, index: String) = {
    val solrUrls = collectionUrls.split(",").toSeq
    val solrClient = new CloudSolrClient.Builder().withSolrUrl(solrUrls).build()

    val queryParamMap = new util.HashMap[String, String]()
    queryParamMap.put("q", searchField + ":" + searchTerm)
    queryParamMap.put("rows", 2147483630.toString)
    val queryParams = new MapSolrParams(queryParamMap)

    val response = solrClient.query(index, queryParams)
    response.getResults
  }
}
