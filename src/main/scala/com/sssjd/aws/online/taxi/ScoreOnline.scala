package com.sssjd.aws.online.taxi

import java.text.SimpleDateFormat
import java.util.Properties

import breeze.linalg.{max, min}
import breeze.numerics.{sqrt, tanh}
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

object ScoreOnline {
  //  val topic = "vdp_roadmatch"
  //  val recvTopic = "vdp_score"
  //  val brokers = "192.168.100.132:9092,192.168.100.183:9092,192.168.100.144:9092"
  val kafkaConf = LoadConfig.getKafkaConfig()


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("scoreStreaming")
      //      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val KafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaConf.get("brokers_taxi").getOrElse().toString,
      "group.id" -> "scoreTaxiEval",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics = List(kafkaConf.get("topic_taxi_roadmatch").getOrElse().toString)
    val message = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, KafkaParams))
    message.foreachRDD(v => fun(v, spark))
    ssc.start()
    ssc.awaitTermination()
  }


  def kafkaProducer(msg: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConf.get("brokers_taxi").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](kafkaConf.get("topic_taxi_score").getOrElse().toString, msg)
    producer.send(record)
    producer.close()
  }


  def funScore(overCnt: (Int, Int, Int, Int), rapidCnt: (Int, Int, Int), timeInterval: Long, initScore: Int = 100): Float = {
    val interval: Float = tanh(timeInterval.toFloat / 3600)
    val overCount = overCnt._1 + 1.5 * overCnt._2 + 2 * overCnt._3 + 2.5 * overCnt._4
    var curScore: Float = initScore - (min(overCount.toInt, 50) + min(rapidCnt._1, 20) + min(rapidCnt._2, 20) + min(rapidCnt._3, 10)) * (1 - interval)
    curScore = sqrt(curScore) * 10
    curScore
  }


  /**
    *
    * @param jedis
    * @param curList
    * @return
    * OverSpeedInfo评分区间超速统计信息
    * taxiRapidCnt评分区间三急统计信息
    * taxiEHstatus记录上下行状态信息(0起点/1上行/2终点/3下行)
    * taxiScore记录总评分
    */

  def overSpeed(jedis: Jedis, curList: List[String]): (Int, Int, Int, Int) = {
    val speeding = jedis.hget("OverSpeedInfo", curList.head).+(":0:2:3:4")
    val over = speeding.split(":").map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.length)).toList.sortBy(_._1)
    val overFirst = over.head._2 - 1
    val overSecond = over(1)._2 - 1
    val overThird = over(2)._2 - 1
    val overFourth = over(3)._2 - 1
    val overCnt = (overFirst, overSecond, overThird, overFourth)
    jedis.hdel("OverSpeedInfo", curList.head)
    overCnt
  }

  def rapidSpeed(jedis: Jedis, curList: List[String]): (Int, Int, Int) = {
    val sudden = jedis.hget("taxiRapidCnt", curList.head).+(",1,2,3")
    val result = sudden.split(",").map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.length)).toList.sortBy(_._1)
    val acc = result.head._2 - 1
    val dec = result(1)._2 - 1
    val ang = result(2)._2 - 1
    val rapidCnt: (Int, Int, Int) = (acc, dec, ang)
    jedis.hdel("taxiRapidCnt", curList.head)
    rapidCnt
  }

  //jsy,Dbuscard,speed,lutc,longtime,lon,lat,EHstatus,carHeadAngle,wayname,wayid,waytype,OverSpeedflag,speedlimit
  def fun(v: RDD[ConsumerRecord[String, String]], spark: SparkSession): Unit = {
    if (!v.isEmpty()) {
      val jedis: Jedis = getJedis()
      try {
        val rdd: RDD[String] = v.map(_.value())
        val array = rdd.collect()

        //      val jedis2 = getJedis()
        //      if(jedis2.get("recv_scorecount")==null){jedis2.set("recv_scorecount","0")}
        //      var cnt = jedis2.get("recv_scorecount").toInt
        //      cnt = cnt + array.length
        //      jedis2.set("recv_scorecount",cnt.toString)
        //      jedis2.close()


        for (ary <- array) {
          val curList = ary.split(",").toList
          val preList = jedis.hget("taxiEHstatus", curList.head)
          if (preList == null) jedis.hset("taxiEHstatus", curList.head, curList(4) + ":" + curList(7))
          else {
            val preEH = preList.split(":").toList

            if (curList(7) != preEH(1) && curList(4) > preEH.head) {
              jedis.hset("taxiEHstatus", curList.head, curList(4) + ":" + curList(7))

              val overCnt: (Int, Int, Int, Int) = overSpeed(jedis, curList)

              val rapidCnt: (Int, Int, Int) = rapidSpeed(jedis, curList)

              val timeInterval: Long = (curList(4).toLong - preEH.head.toLong) / 1000
              val minuteInterval = (timeInterval.toFloat / 60).formatted("%.1f")

              val curScore: Float = funScore(overCnt, rapidCnt, timeInterval)

              val getScore = jedis.hget("taxiScore", curList.head)
              val totScore = if (getScore == null) curScore else getScore.toFloat
              val lastScore = ((totScore + curScore) / 2).formatted("%.2f")
              jedis.hset("taxiScore", curList.head, lastScore.toString)
              val msg = curList(3) + "," + curList.head + "," + curScore.formatted("%.2f") + "," + lastScore + "," + overCnt._1 + "," + overCnt._2 + "," + overCnt._3 + "," + overCnt._4 + "," + rapidCnt._1 + "," + rapidCnt._2 + "," + rapidCnt._3 + "," + minuteInterval
              println(msg)
              kafkaProducer(msg)
            }
          }
        }
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        retJedis(jedis)
      }
    }
  }


}


