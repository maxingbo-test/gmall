package com.gmall.realtime.util

import java.util
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis


object OffsetManager {

  // 从redis中读取offset
  // 传入：topic，groupid  输出：topic-partition offset起始点
  def getOffset(topicName:String,groupId:String): Map[TopicPartition,Long] ={
      // redis  type：hash  key：offset:[topic]:[groupid]  key_1：partition_id  value：offset
      val jedis:Jedis = RedisUtil.getJedisClient
      val offsetKey = "offset:"+topicName+":"+groupId
      // hgetall 获取所有 小key value
      val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
      jedis.close()
      import scala.collection.JavaConversions._
      val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map {
          case (partitionId, offset) =>
              (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }.toMap
      kafkaOffsetMap
  }

  // 偏移量写入redis
  // 输入：topic，groupid，offset结束点
  def saveOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange]): Unit ={
      // redis  type：hash  key：offset:[topic]:[groupid]  key_1：partition_id  value：offset
      val offsetKey = "offset:"+topicName+":"+groupId
      val offsetMap:util.Map[String,String] = new util.HashMap()
      for(offset <- offsetRanges){
          val partition: Int = offset.partition
          val untilOffset: Long = offset.untilOffset
          offsetMap.put(partition.toString,untilOffset.toString)
          println("topic:"+topicName+"  groupid:"+groupId+"  偏移量起始位置:"+offset.fromOffset+"  偏移量结束位置："+untilOffset)
      }
      // 写入redis
      if(offsetMap != null && offsetMap.size() > 0){
          val jedis:Jedis = RedisUtil.getJedisClient
          jedis.hmset(offsetKey,offsetMap)
          jedis.close()
      }

  }

}
