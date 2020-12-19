package com.gmall.realtime.util

import java.util
import com.gmall.realtime.util.AllCaseClass.DauInfo
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

object MyEsUtils {

    // es连接池
    var factory:JestClientFactory = null;

    def getClient:JestClient = {
      if(factory == null) build()
      factory.getObject
    }

    def build():Unit = {
      factory = new JestClientFactory
      factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop1:9200") // 连接位置
        .multiThreaded(true) // 支持多线程
        .maxTotalConnection(20) // 最多连接并发数
        .connTimeout(10000) // 连接超时
        .readTimeout(10000) // 读取超时
        .build())
    }

    case class obj(ip:String,method:String)

    // 增加文档
    def addDoc():Unit = {
      var jest:JestClient = getClient

      val index = new Index.Builder(obj("192.168.0.101","get method"))
        .index("product14")
        .`type`("doc")
        .id("0105")
        .build()

      val message = jest.execute(index).getErrorMessage
      if(message != null)
        println(message)

      jest.close()
    }

    // 批量插入
    // 传入一个元祖类型
    def bulkDoc(sourceList:List[(String,DauInfo)],indexName:String):Unit = {
      if(sourceList.size != null && sourceList.size > 0){
        val jest: JestClient = getClient
        // 批次操作
        val bulkBuilder = new Bulk.Builder

        for((id,source) <- sourceList){
          // 多个index组成一个批量的bulk
          val index: Index = new Index.Builder(source).index(indexName)
                                      .`type`("_doc")
                                      .id(id).build()
          bulkBuilder.addAction(index)
        }
        val bulk: Bulk = bulkBuilder.build()
        val result: BulkResult = jest.execute(bulk)
        println("查看保存到es多少条数据:"+result.getItems.size())
        jest.close()
      }
    }

    // 查询文档
    def queryDoc():Unit = {
      val jest = getClient

      val query =
        """
          |{
          |  "query": {
          |    "bool": {
          |      "must": [
          |        {
          |          "match": {
          |            "name": "red"
          |          }
          |        }
          |      ],
          |      "filter": {
          |        "term": {
          |          "actorList.name.keyword": "zhang yi"
          |        }
          |      }
          |    }
          |  },
          |  "from": 0
          |  ,"size": 20
          |  ,"sort": [
          |    {
          |      "doubanScore": {
          |        "order": "desc"
          |      }
          |    }
          |  ],
          |  "highlight": {
          |    "fields": {
          |      "name": {}
          |    }
          |  }
          |}
        """.stripMargin

      val builder = new SearchSourceBuilder
      val bool = new BoolQueryBuilder
      bool.must(new MatchQueryBuilder("name","red00"))
      bool.filter(new TermQueryBuilder("actorList.name.keyword","zhang yi"))
      builder.query(bool)
      builder.from(0)
      builder.size(20)
      builder.sort("doubanScore",SortOrder.DESC)
      builder.highlight(new HighlightBuilder().field("name"))
      val query2 = builder.toString

      val search = new Search.Builder(query2)
        .addIndex("movie_index")
        .addType("movie")
        .build()

      val result:SearchResult = jest.execute(search)
      val hits:util.List[SearchResult#Hit[util.Map[String,Any],Void]]
      = result.getHits(classOf[util.Map[String,Any]])
      import scala.collection.JavaConversions._
      for(hit <- hits){
        println(hit.source.mkString(","))
      }
      jest.close()
    }


    def main(args: Array[String]): Unit = {
      queryDoc()
    }

}
