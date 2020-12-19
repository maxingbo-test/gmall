package com.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gmall.realtime.util.AllCaseClass.DauInfo
import com.gmall.realtime.util.{MyEsUtils, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {

      // kafka 4个分区,一般会与 spark 分区一致
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
      // 5秒获取一次数据
      val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
      val topic = "gmall_start"
      val groupId = "dau_group"

      // redis 获取 kafka offset 起始点
      val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
      var kafkaInputStream: InputDStream[ConsumerRecord[String, String]] = null
      if(kafkaOffsetMap != null && kafkaOffsetMap.size > 0){
          // 获取kafka数据，有offset状态（从redis中获取offset）
          kafkaInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
      } else {
          // 获取kafka数据，启初无offset状态
          kafkaInputStream = MyKafkaUtil.getKafkaStream(topic,ssc)
      }

      // 获取 kafka offset 结束点
      // OffsetRange 是每个 partition 结束点 offset，用于更新 redis 中的 kafka 偏移量
      var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
      val getOffsetDs = kafkaInputStream.transform { rdd =>
          // rdd不变，就是为了获取结束位置
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
      }

      // getOffsetDs 做了转换，一定要有action操作才行，接下面的map

      // 数据格式化，补充了两个字段
      val jsonObjectDs: DStream[JSONObject] = getOffsetDs.map(x => {
          val jsonStr: String = x.value()
          val jsonObj: JSONObject = JSON.parseObject(jsonStr)
          val ts: lang.Long = jsonObj.getLong("ts")
          val dateHourString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
          val dateHour: Array[String] = dateHourString.split(" ")
          jsonObj.put("dt", dateHour(0))
          jsonObj.put("hr", dateHour(1))
          jsonObj
        }
      )

      /**
      * {"common":{"ar":"370000","ba":"Xiaomi","ch":"huawei","md":"Xiaomi 10 Pro "
      * ,"mid":"mid_11","os":"Android 10.0","uid":"180","vc":"v2.1.134"}
      * ,"start":{"entry":"notice","loading_time":13008,"open_ad_id":18
      * ,"open_ad_ms":3082,"open_ad_skip_ms":0},"ts":1585728677000}
      */
      // 去重，用 redis 保存今天访问过系统的用户清单
      // 在 redis 中保存清单（type：set ， key：dau:2020-07-01  ， value：mid  ， expire：24小时）

      //     方式一：filter过滤（缺点:频繁调用redis连接，取一次调用一次）
      //     jsonObjectDs.filter(jsonObj => {
      //        val dateStr: String = jsonObj.getString("dt")
      //        val mid: String = jsonObj.getJSONObject("common").getString("mid")
      //        val jedis: Jedis = RedisUtil.getJedisClient
      //        val rkey = "dau:"+dateStr
      //        val isNew: lang.Long = jedis.sadd(rkey,mid)
      //        jedis.close()
      //        if(isNew == 1L)
      //            true
      //        else
      //            false
      //     })

      //     方式二：以分区为单位的过滤方法（单独map不可以过滤的，redis连接一个分区申请一次）
      //     mapPartitions 过滤思路：提供一个集合(iterator,每个分区迭代器)，加工后输出一个结果集
      val filteredDstream = jsonObjectDs.mapPartitions(jsonObjItr => {
          val jedis: Jedis = RedisUtil.getJedisClient
          val filterList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
          // jsonObjItr.size 集合只能操作一次，这里操作，下面就不能运行了
          // jsonObjItr 转成一个对象，就可以操作了
          val jsonList: List[JSONObject] = jsonObjItr.toList
          println("过滤前："+jsonList.size)
          for(jsonObj <- jsonList){
              // 遍历分区中的每一条数据
              val dateStr: String = jsonObj.getString("dt")
              val mid: String = jsonObj.getJSONObject("common").getString("mid")
              val rkey = "dau:"+dateStr
              val isNew: lang.Long = jedis.sadd(rkey,mid)
              // 设置key过期时间
              jedis.expire(rkey,3600*24)
              if(isNew == 1L)
                filterList+=jsonObj
          }
          println("过滤后："+filterList.size)
          jedis.close()
          filterList.toIterator
      })

      // sparkStreaming 过滤后插入 es
      filteredDstream.foreachRDD { rdd =>
          rdd.foreachPartition { jsonIter =>
              val list: List[JSONObject] = jsonIter.toList

              // 源数据转成要保存的数据格式
              val dauList: List[(String,DauInfo)] = list.map { jsonObj =>
                  val obj: JSONObject = jsonObj.getJSONObject("common")
                  val dauinfo = DauInfo(obj.getString("mid")
                      , obj.getString("uid")
                      , obj.getString("ar")
                      , obj.getString("ch")
                      , obj.getString("vc")
                      , jsonObj.getString("dt")
                      , jsonObj.getString("hr")
                      , jsonObj.getLong("ts"))

                  (dauinfo.dt+"_"+dauinfo.mid,dauinfo)
              }
              val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
              MyEsUtils.bulkDoc(dauList, "gmall_dau_info_"+dateStr)
          }

          // redis 存入 kafka offset 结束点
          // offset 包含了所有分区的offset记录，所以在foreachPartition外面
          OffsetManager.saveOffset(topic,groupId,offsetRanges)
      }

      ssc.start()
      ssc.awaitTermination()
  }

}
