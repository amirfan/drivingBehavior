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
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object rapidIdentification {
  //0.661721	0.687157	0.712594	0.738031	0.763468
  val acceThreshold = 0.763468
  val deceThreshold = -0.763468
  val speedLimit = 20.5
  val angleLimit = 11.5

  //  val acceThreshold = 0.01
  //  val deceThreshold = -0.01
  //  val speedLimit = 0.01
  //  val angleLimit = 0.01
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS")
  val kafkaConf = LoadConfig.getKafkaConfig()


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rapidIdentificationStreaming")
            .master("local[*]")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.default.parallelism", 100)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.release.num.duration", 5)
      .config("spark.streaming.blockInterval", 10)
      .config("spark.streaming.concurrentJobs", 5)
      .config("spark.locality.wait", 100)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val KafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaConf.get("brokers_taxi").getOrElse().toString,
      "group.id" -> "rapidSpeed20190524",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics = List(kafkaConf.get("topic_taxi_roadmatch").getOrElse().toString)

    val message = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, KafkaParams))

    message.foreachRDD(rdd => fun(rdd))

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 三急实时明细
    *
    * @param msg
    */
  def detailsProducer(msg: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConf.get("brokers_taxi").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](kafkaConf.get("topic_taxi_rapid").getOrElse().toString, msg)
    producer.send(record)
    producer.close()
  }

  /**
    * 按趟数发送状态到kafka
    *
    * @param msg
    */

  def statusProducer(msg: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConf.get("brokers_taxi").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](kafkaConf.get("vdp_ridestatus").getOrElse().toString, msg)
    producer.send(record)
    producer.close()
  }


  /**
    * 急加速/急减速/急转弯
    * preList(0):时间戳pretime  preList(1):速度  preList(2):方向
    * curList(3):时间curtime   curList(4):时间戳  curList(2):速度  curList(8):方向
    * taxiPreGps存储上一个GPS信息 K(jsy)->V(时间,速度,角度)
    * taxiPreStatus记录上一时刻三急状态(0/1/2/3)
    * taxiRapidDetail记录评分区间三急数目
    *
    */


  def fun(rdd: RDD[ConsumerRecord[String, String]]): Unit = {

    val dt0 = new Date().getTime()

    if (!rdd.isEmpty()) {
      println("------------------")

      rdd.foreachPartition { partitionRDD =>

        val jedis: Jedis = getJedis()

        try {
          partitionRDD.foreach(row => {
            val dt = new Date().getTime()
            val curList = row.value().split(",").toList

            var preList = List[String]()

            val preString = jedis.hget("taxiPreGps", curList.head)

            if (preString == null) {
              jedis.hset("taxiPreGps", curList.head, curList(4).+(",").+(curList(2).+(",").+(curList(8))))
            }
            else {
              preList = preString.split(",").toList
              if (preList.length == 3) {
                jedis.hset("taxiPreGps", curList.head, preList.head.+(",").+(preList(1)).+(",").+(preList(2)).+(",").+(curList(4)).+(",").+(curList(2)).+(",").+(curList(8)))
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
                case _ if angle > 180.0 => (360.0 - angle) / timeInterval
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

              val dt2 = new Date().getTime()


              val bl = new StringBuffer()
              bl.append(backList(1).+(",").+(backList(2)).+(",").+(backList(4)).+(",").+(backList(6)).+(",").+(backList(7)).+(",").+(backList(3)).+(","))

              val mse = backList.head match {
                case "3" => bl.append(avgSpeed.toString.+(",").+(timeInterval.toString).+(",").+(angTmp.toString).+(",").+(backList.head))
                case _ => bl.append(preAvgSpeed.toString.+(",").+(timeInterval.toString).+(",").+(angTmp.toString).+(",").+(backList.head))
              }

              val msg = mse.append(",".+(backList(backList.size - 1)).+(",").+(dt0).+(",").+(dt).+(",").+(dt2).+(",").+(new Date().getTime())).toString
              detailsProducer(msg)



//              val msg = backList.head match {
//                case "3" => backList(1).+(",").+(backList(2)).+(",").+(backList(4)).+(",").+(backList(6)).+(",").+(backList(7)).+(",").+(backList(3)).+(",").+(avgSpeed.toString).+(",").+(timeInterval.toString).+(",").+(angTmp.toString).+(",").+(backList.head).+(",").+(backList(backList.size - 1)).+(",").+(dt0).+(",").+(dt).+(",").+(dt2).+(",").+(new Date().getTime())
//                case _ => backList(1).+(",").+(backList(2)).+(",").+(backList(4)).+(",").+(backList(6)).+(",").+(backList(7)).+(",").+(backList(3)).+(",").+(preAvgSpeed.toString).+(",").+(timeInterval.toString).+(",").+(angTmp.toString).+(",").+(backList.head).+(",").+(backList(backList.size - 1)).+(",").+(dt0).+(",").+(dt).+(",").+(dt2).+(",").+(new Date().getTime())
//              }
//              detailsProducer(msg)

              /**
                * 计算统计明细，写入redis
                */

              if (backList.head != "0") {
                var mse = jedis.hget("taxiRapidDetail", backList(1))
                if (mse == null) {
                  mse = backList.head
                } else {
                  mse = mse.+(":").+(backList.head)
                }
                jedis.hset("taxiRapidDetail", backList(1), mse)

              }
            }

            /**
              * 计算行驶结束
              */
            val preStatus = jedis.hget("taxiEHstatus", curList.head)
            if (preStatus == null) {
              jedis.hset("taxiEHstatus", curList.head, curList(4) + ":" + curList(7))
            }
            else {
              val preEH = preStatus.split(":").toList

              if (curList(7) != preEH(1) && curList(4) > preEH.head) {

                jedis.hset("taxiEHstatus", curList.head, curList(4) + ":" + curList(7))

//                if ((curList(7) == "2" && preEH(1) == "1") || (curList(7) == "0" && preEH(1) == "3")) {
                  /**
                    * 发送三急统计值到redis,成功则发送状态消息到kafka,同时清空redis明细
                    */
                  val cnt = Option(jedis.hget("taxiRapidDetail", curList.head)) match {
                    case Some(s) => s
                    case _ => "0"
                  }

                  // uid_beginTime_endTime
                  val scoreKey = curList.head + "_" + preEH.head + "_" + curList(4)
                  println(scoreKey)
                  println(cnt)
                  jedis.hset("taxiRapidCnt", scoreKey, cnt)
                  //                  statusProducer(scoreKey)
                  jedis.hdel("taxiRapidDetail", curList.head)

//                }
              }
            }

          })
        }

        catch {
          case ie: InterruptedException => Thread.currentThread().interrupt()
          case e: Exception => e.printStackTrace()
        }
        finally {
          retJedis(jedis)
        }
      }
    }
  }
}