package com.sssjd.backup.onlineBK.taxiQuantile

import java.util.Properties

//import breeze.numerics.{abs, cos, pow, sin}
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.math._

object RapidEvent extends EventStatus {
  //0.661721	0.687157	0.712594	0.738031	0.763468
    val acceThreshold = 0.2438
    val deceThreshold = -0.2381
    val speedLimit = 29.843
    val angleLimit = 2.1968

  val kafkaConf = LoadConfig.getKafkaConfig()


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("rapidIdentificationStreaming")
      .master("local[*]")
      .config("spark.default.parallelism", 1000)
      .config("spark.streaming.concurrentJobs", 10)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.release.num.duration", 5)
      .config("spark.streaming.blockInterval", 10)
      .config("spark.locality.wait", 100)
      .config("spark.streaming.kafka.consumer.cache.enabled",false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val KafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaConf.get("brokers_bus").getOrElse().toString,
      "group.id" -> "rapidBusConsumerTEST111",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics = List(kafkaConf.get("topic_bus_roadmatch").getOrElse().toString)

    val message = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, KafkaParams))

    message.foreachRDD(rdd => fun(rdd))
    ssc.start()
    ssc.awaitTermination()
  }


    //三急实时明细
  def detailsProducer(msg: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConf.get("brokers_bus").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](kafkaConf.get("topic_bus_rapid").getOrElse().toString, msg)
    producer.send(record)
    producer.close()
  }


  //按趟数发送状态到kafka
  def statusProducer(msg: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConf.get("brokers_bus").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](kafkaConf.get("vdp_ridestatus").getOrElse().toString, msg)
    producer.send(record)
    producer.close()
  }

  /**
    * 经纬度计算距离
    * @param lut1上一时刻经度lon
    * @param lat1上一时刻纬度lat
    * @param lut2经度Lon
    * @param lat2纬度lat
    */
  def getDist(lut1:Double,lat1:Double,lut2:Double,lat2:Double): Double ={

    val rad:Double = 6378.137
    val lts = sin(pow(abs(lat1 - lat2)*Pi/360,2))
    val lns = sin(pow(abs(lut1 - lut2)*Pi/360,2))
    val ltc = cos(lat1 * Pi /180) * cos(lat2 * Pi / 180)
    val distance = 2 * asin(sqrt(lts + lns * ltc)) * rad
    distance * 1000
  }



  /**
    * 急加速/急减速/急转弯
    * preList(0):时间戳pretime  preList(1):速度  preList(2):方向
    * curList(3):时间curtime   curList(4):时间戳  curList(2):速度  curList(8):方向
    * taxiPreGps存储上一个GPS信息 K(jsy)->V(时间,速度,角度) ---------(时间,速度,经度,纬度,角度)
    * taxiPreStatus记录上一时刻三急状态(0/1/2/3)
    * taxiRapidDetail记录评分区间三急数目
    *
    */

  def fun(records: RDD[ConsumerRecord[String, String]]): Unit = {
    if (!records.isEmpty()) {

      records.foreachPartition { partitionRDD =>

        val jedis: Jedis = getJedis()

        try {
          partitionRDD.foreach(row => {

            val curList = row.value().split(",").toList
            val jsy = curList.head

            val curGps = List(curList(4), curList(5), curList(6), curList(2), curList(8))

            var preGPS = List[String]()
            val preString = jedis.hget("busPreGps", jsy)

            if (preString == null) {

              jedis.hset("busPreGps", jsy, curGps.mkString(","))
            }
            else {

              preGPS = preString.split(",").toList

              val gps = preGPS.takeRight(5) ++ curGps

              jedis.hset("busPreGps", jsy, gps.mkString(","))
            }

            if (preGPS.length == 10 && preGPS.head < preGPS(5) && preGPS(5) < curGps.head) {

              val timeInterval: Long = (curGps.head.toLong - preGPS(5).toLong) / 1000 // 时间间隔:s

              val totInterval = (curGps.head.toLong - preGPS.head.toLong) / 1000 //时间总间隔
              val preInterval = (preGPS(5).toLong - preGPS.head.toLong) / 1000 //前一时间间隔

              val sudSpeed = (curGps(3).toFloat - preGPS(8).toFloat) / (timeInterval * 3.6) //加速度m/s^2
              val preSudden = abs((preGPS(8).toFloat - preGPS(3).toFloat) / (preInterval * 3.6))
              val negSudden = 0 - preSudden

              val preAvgSpeed = (preGPS(3).toFloat + preGPS(8).toFloat) / 2 //上一时刻速度km/h

              val avgSpeed = (curGps(3).toFloat + preGPS(8).toFloat) / 2 //理论平均速度km/h
              val theroyDistance = avgSpeed * timeInterval / 3.6 //理论里程
              val actualDistance = getDist(preGPS(6).toDouble, preGPS(7).toDouble, curGps(1).toDouble, curGps(2).toDouble) //实际行驶里程
              val actualSpeed = actualDistance / timeInterval * 3.6 //实际平均速度km/h

              val angTmp = abs(curGps(4).toFloat - preGPS(9).toFloat)
              val angle = angTmp match { //  角度变化
                case _ if angTmp > 180.0 => 360.0 - angTmp
                case _ => angTmp
              }
              val angSpeed = angle / timeInterval // 角速度

              if (preInterval <= 20) {

                //条件1：加速度大于阈值
                //条件2：角速度大于阈值&&角度大于阈值&&速度大于阈值

                var stat =
                  if (sudSpeed > acceThreshold ) curList ++ List("1")
                  else if (sudSpeed < deceThreshold ) curList ++ List("2")
                  else if (angSpeed > angleLimit && angle>45 && avgSpeed > speedLimit) curList ++ List("3")
                  else curList ++ List("0")


                //过滤连续状态
                val st = stat.takeRight(1).mkString("")
                val status = jedis.hget("busPreStatus", jsy)
                if (st == status) stat = curList ++ List("0")
                else jedis.hset("busPreStatus", jsy, st)

                val st2 = stat.takeRight(1).mkString("")

                //事件判断
                if (!st2.equals("0")) {
                  val stat2 = eventStatus(st2, jsy, preGPS, curGps, curList, jedis)
                  if(stat2 != null){
                    println(stat2)
                    println("-------------------------------")
                  }
                }

                //计算统计明细，写入redis
                if (st2 != "0") {
                  var mse = jedis.hget("busRapidDetail", jsy)
                  if (mse == null) {
                    mse = st2
                  } else {
                    mse = mse + ":" + st2
                  }
                  jedis.hset("busRapidDetail", jsy, mse)

                  //发送kafka(jsy,cph,tm,lut,lat,speed,preSpeed,theroySpeed,actualSpeed,theroyMile,actualMile,tmInterval,angle,st)
                  val bsc: List[String] = List(stat.head, stat(1), stat(3), stat(5), stat(6))
                  val spd: List[String] = List(preGPS(3), preGPS(8), stat(2))
                  val mle: List[String] = List(theroyDistance.formatted("%.2f"), actualDistance.formatted("%.2f"), avgSpeed.toString, actualSpeed.formatted("%.2f"), timeInterval.toString, angle.toString, st2)
                  val msg = (bsc ++ spd ++ mle).mkString(",")
                  println(msg)
                  detailsProducer(msg)
                }

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