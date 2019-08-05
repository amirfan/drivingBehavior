package com.sssjd.backup.onlineBK.bus

import java.util.Properties

import breeze.linalg.min
import breeze.numerics.{sqrt, tanh}
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

object RealtimeScore {

  val kafkaConf = LoadConfig.getKafkaConfig()

  val topicMsg = mutable.HashMap[String, String]()


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scoreStreaming")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val KafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaConf.get("brokers_bus").getOrElse().toString,
      "group.id" -> "scoreBusEval",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics = List(kafkaConf.get("vdp_ridestatus").getOrElse().toString)
    val message = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, KafkaParams))
    message.foreachRDD(rdd => fun(rdd))
    ssc.start()
    ssc.awaitTermination()
  }


  def kafkaProducer(msg: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConf.get("brokers_bus").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](kafkaConf.get("topic_bus_score").getOrElse().toString, msg)
    producer.send(record)
    producer.close()
  }


  def funScore(overCnt: (Int, Int, Int, Int), rapidCnt: (Int, Int, Int), timeInterval: Long, initScore: Int = 100): Float = {
    val interval: Float = tanh(timeInterval.toFloat / 3600)
    val overCount = overCnt._1 + 1.5 * overCnt._2 + 2 * overCnt._3 + 2.5 * overCnt._4
    var curScore: Float = (initScore - min(overCount.toInt, 50) - min(rapidCnt._1, 20) - min(rapidCnt._2, 20) - min(rapidCnt._3, 10)) * interval
    curScore = sqrt(curScore) * 10
    curScore
  }

  /**
    *
    * @param jedis
    * @param curList
    * @return
    * OverSpeedInfo评分区间超速统计信息
    * busRapidCnt评分区间三急统计信息
    * busEHstatus记录上下行状态信息(0起点/1上行/2终点/3下行)
    * busScore记录总评分
    */

  def overSpeed(jedis: Jedis, speeding: String): (Int, Int, Int, Int) = {

    val over = speeding.split(":").map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.length)).toList.sortBy(_._1)
    val overFirst = over.head._2 - 1
    val overSecond = over(1)._2 - 1
    val overThird = over(2)._2 - 1
    val overFourth = over(3)._2 - 1
    val overCnt = (overFirst, overSecond, overThird, overFourth)
    overCnt
  }

  def rapidSpeed(jedis: Jedis, sudden: String): (Int, Int, Int) = {
    val result = sudden.split(":").map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.length)).toList.sortBy(_._1)
    val acc = result.head._2 - 1
    val dec = result(1)._2 - 1
    val ang = result(2)._2 - 1
    val rapidCnt: (Int, Int, Int) = (acc, dec, ang)
    rapidCnt
  }


  def fun(rdd: RDD[ConsumerRecord[String, String]]): Unit = {

    if (!rdd.isEmpty()) {

      val jedis: Jedis = getJedis()

      try {
        val v: RDD[String] = rdd.map(_.value())
        val array = v.collect()
        for (ary <- array) {
          val curList = ary.split("_").toList

          if (topicMsg.contains(curList(0))) {
            val status = topicMsg.getOrElse(curList(0), "0")
            if (status.equals(curList(2))) {
              /**
                * 读取redis获取超速、三急数据,进行评分，删除缓存数据
                */

              val overDetail = jedis.hget("OsRideSummary", ary).+(":0:4:5:6")
              val rapidDetail = jedis.hget("busRapidCnt", ary).+(":1:2:3")
              val overCnt = overSpeed(jedis, overDetail)
              val rapidCnt = rapidSpeed(jedis, rapidDetail)

              val timeInterval = (curList(2).toLong - curList(1).toLong)/1000
              val minuteInterval = (timeInterval.toFloat / 60).formatted("%.1f")

              val curScore = funScore(overCnt, rapidCnt, timeInterval)
              val getScore = jedis.hget("busScore", curList.head)
              val totScore = if (getScore == null) curScore else getScore.toFloat
              val lastScore = ((totScore + curScore) / 2).formatted("%.2f")
              jedis.hset("busScore", curList.head, lastScore.toString)
              val msg = curList(2) + "," + curList.head + "," + curScore.formatted("%.2f") + "," + lastScore + "," + overCnt._1 + "," + overCnt._2 + "," + overCnt._3 + "," + overCnt._4 + "," + rapidCnt._1 + "," + rapidCnt._2 + "," + rapidCnt._3 + "," + minuteInterval
              println(msg)
              kafkaProducer(msg)

              jedis.hdel("OsRideSummary", "uid_beginTime_endTime")
              jedis.hdel("busRapidCnt", "uid_beginTime_endTime")

            } else {
              println(topicMsg)
              topicMsg.update(curList(0), curList(2))
            }

          } else {
            println(topicMsg)
            println(curList(0))
            topicMsg.put(curList(0), curList(2))
          }
        }

      } catch {
        case ie: InterruptedException => Thread.currentThread().interrupt()
        case e: Exception => e.printStackTrace()
      } finally {
        retJedis(jedis)
      }

    }

  }
}






