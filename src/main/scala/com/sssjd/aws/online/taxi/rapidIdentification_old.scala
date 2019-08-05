package com.sssjd.aws.online.taxi

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import breeze.numerics.abs
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object rapidIdentification_old {
  //0.661721	0.687157	0.712594	0.738031	0.763468
  //    val acceThreshold = 0.763468
  //    val deceThreshold = -0.763468
  //    val speedLimit = 20.5
  //    val angleLimit = 11.5

  val acceThreshold = 0.01
  val deceThreshold = -0.01
  val speedLimit = 0.01
  val angleLimit = 0.01
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS")
  val jedis: Jedis = getJedis()
  val kafkaConf = LoadConfig.getKafkaConfig()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rapidIdentificationStreaming")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val KafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaConf.get("brokers_taxi").getOrElse().toString,
      "group.id" -> "rapidTaxiConsumer",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics = List(kafkaConf.get("topic_taxi_roadmatch").getOrElse().toString)

    val message: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
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
    val record = new ProducerRecord[String, String](kafkaConf.get("topic_taxi_rapid").getOrElse().toString, msg)
    println(record)
    producer.send(record)
    producer.close()
  }


  /**
    * 急加速/急减速/急转弯
    * preList(0):时间戳pretime  preList(1):速度  preList(2):方向
    * curList(3):时间curtime   curList(4):时间戳  curList(2):速度  curList(8):方向
    * taxiPreGps存储上一个GPS信息 K(jsy)->V(时间,速度,角度)
    * taxiPreStatus记录上一时刻三急状态(0/1/2/3)
    * taxiRapidCnt记录评分区间三急数目
    *
    */


  def fun(v: RDD[ConsumerRecord[String, String]], spark: SparkSession): Unit = {

    val dt2 = dateFormat.parse(dateFormat.format(new Date())).getTime()

    if (!v.isEmpty()) {


      //      jedis.del("taxiPreGps")
      try {
        val rdd: RDD[String] = v.map(_.value())
        val array = rdd.collect()
        for (ary <- array) {
          //          println(ary)

          val curList = ary.split(",").toList

          var preList = List[String]()
          val preString = jedis.hget("taxiPreGps", curList.head)
          if (preString == null) {
            jedis.hset("taxiPreGps", curList.head, curList(4).+(",").+(curList(2).+(",").+(curList(8))))
          } else {
            preList = preString.split(",").toList
            if (preList.length == 3) {
              jedis.hset("taxiPreGps", curList.head, preList(0).+(",").+(preList(1)).+(",").+(preList(2)).+(",").+(curList(4)).+(",").+(curList(2)).+(",").+(curList(8)))
            } else {
              jedis.hset("taxiPreGps", curList.head, preList(3).+(",").+(preList(4)).+(",").+(preList(5)).+(",").+(curList(4)).+(",").+(curList(2)).+(",").+(curList(8)))
            }
          }

          /**
            * (pptime,ppspeed,ppturn,ptime,pspeed,pturn)(jsy,carID,speed,tm,time,lut,lat,eh,turn,...)
            * **************************************************************************
            */

          if (preList.length == 6 && preList.head != curList(4) && preList(3) != curList(4)) {

            val timeInterval: Long = (curList(4).toLong - preList(3).toLong) / 1000
            val sudSpeed = (curList(2).toFloat - preList(4).toFloat) / (timeInterval * 3.6)
            val avgSpeed = (curList(2).toFloat + preList(4).toFloat) / 2
            val angle = abs(curList(8).toFloat - preList(5).toFloat)
            val angSpeed = angle match {
              case _ if (angle > 180.0) => (360.0 - angle) / timeInterval
              case _ => angle / timeInterval
            }

            val preAvgSpeed = (preList(1).toFloat + preList(4).toFloat) / 2
            val angTmp = angSpeed * timeInterval

            var backList =
              if (sudSpeed > acceThreshold && curList(2).toFloat > speedLimit) curList.+:("1")
              else if (sudSpeed < deceThreshold && preList(4).toFloat > speedLimit) curList.+:("2")
              else if (angSpeed > angleLimit && avgSpeed > speedLimit) curList.+:("3")
              else curList.+:("0")

            val status = jedis.hget("taxiPreStatus", curList.head)
            if (backList.head == status) backList = curList.+:("0")
            else jedis.hset("taxiPreStatus", curList.head, backList.head)


            //(jsy,carID,speed,tm,time,lut,lat,eh,turn,...)
            val msg = backList.head match {
              case "3" => backList(1).+(",").+(backList(2)).+(",").+(backList(4)).+(",").+(backList(6)).+(",").+(backList(7)).+(",").+(backList(3)).+(",").+(avgSpeed.toString).+(",").+(timeInterval.toString).+(",").+(angTmp.toString).+(",").+(backList.head).+(",").+(backList(backList.size - 1)).+(",").+(dt2).+(",").+(dateFormat.parse(dateFormat.format(new Date())).getTime())
              case _ => backList(1).+(",").+(backList(2)).+(",").+(backList(4)).+(",").+(backList(6)).+(",").+(backList(7)).+(",").+(backList(3)).+(",").+(preAvgSpeed.toString).+(",").+(timeInterval.toString).+(",").+(angTmp.toString).+(",").+(backList.head).+(",").+(backList(backList.size - 1)).+(",").+(dt2).+(",").+(dateFormat.parse(dateFormat.format(new Date())).getTime())
            }
            println(msg)
            kafkaProducer(msg)


            if (backList.head != "0") {
              var mse = jedis.hget("taxiRapidCnt", backList(1))
              if (mse == null) {
                mse = backList.head
              } else {
                mse = mse.+(",").+(backList.head)
              }
              jedis.hset("taxiRapidCnt", backList(1), mse)


            }
          }
        }
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        //retJedis(jedis)
      }
    }
  }

}
