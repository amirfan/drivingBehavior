package com.sssjd.backup.eventBK

import java.util.Properties

import com.sssjd.configure.LoadConfig
import UDFDistanceBK._
import com.sssjd.backup.taxiBK.EventStatus
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.math.abs

object RapidEventBK extends EventStatus {

  val acceThreshold = 0.885
  val deceThreshold = -0.878
  val mileageInterval_mi = 26.256
  val mileageInterval_mx = 41.484
  val speedLimit = 33.79 // --km/h
  val angleLimit = 45
  val angularSpeedLimit = 20.486


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
    * 急加速/急减速/急转弯
    * preList(0):时间戳pretime  preList(1):速度  preList(2):方向
    * curList(3):时间curtime   curList(4):时间戳  curList(2):速度  curList(8):方向
    * taxiPreGps存储上一个GPS信息 K(jsy)->V(时间,速度,角度) ---------(时间,速度,经度,纬度,角度)
    * taxiPreStatus记录上一时刻三急状态(0/1/2/3)
    * taxiRapidDetail记录评分区间三急数目
    *
    * * preGPS(pptime,pplut,pplat,ppspeed,ppturn,ptime,plut,plat,pspeed,pturn)
    * * curGps(time,lut,lat,speed,turn)
    *
    */

  def fun(records: RDD[ConsumerRecord[String, String]]): Unit = {
    if (!records.isEmpty()) {

      records.foreachPartition { partitionRDD =>

        var jedis: Jedis = null

        try {
          jedis = getJedis()
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
              val avgSpeed = (curGps(3).toFloat + preGPS(8).toFloat) / 2 //理论平均速度km/h
              val sudSpeed = (curGps(3).toFloat - preGPS(8).toFloat) / (timeInterval * 3.6) //加速度m/s^2

              val theroyUpMin = theroyDistUpMin(preGPS(8).toDouble,curGps(3).toDouble,timeInterval) //假设急加状态下行驶里程最小值
              val theroyUpMax = theroyDistUpMax(preGPS(8).toDouble,curGps(3).toDouble,timeInterval) //假设急加状态下行驶里程最大值
              val theroyDnMin = theroyDistDnMin(preGPS(8).toDouble,curGps(3).toDouble,timeInterval) //假设急减状态下行驶里程最小值
              val theroyDnMax = theroyDistDnMax(preGPS(8).toDouble,curGps(3).toDouble,timeInterval) //假设急减状态下行驶里程最大值
              val actualMileage = getDist(preGPS(6).toDouble, preGPS(7).toDouble, curGps(1).toDouble, curGps(2).toDouble) //实际行驶里程
              val theryAvgMileage = avgSpeed/3.6 * timeInterval   //理论平均速度下行驶里程
              val mileageInterval = abs(actualMileage - theryAvgMileage) //里程差

              val angTmp = abs(curGps(4).toFloat - preGPS(9).toFloat)
              val angle = angTmp match { //  角度变化
                case _ if angTmp > 180.0 => 360.0 - angTmp
                case _ => angTmp
              }
              val angularSpeed = angle / timeInterval // 角速度

              if (timeInterval <= 60 && timeInterval>5) {

                //条件1：加速度大于阈值 && 里程大于/小于临界值
                //条件2：角速度大于阈值&&角度大于阈值&&速度大于阈值


                var stat =
                  if (sudSpeed >acceThreshold && (mileageInterval>mileageInterval_mi && mileageInterval<mileageInterval_mx) && ( actualMileage > theroyUpMax || actualMileage < theroyUpMin )) curList ++ List("1")
                  else if (sudSpeed < deceThreshold && (mileageInterval>mileageInterval_mi && mileageInterval<mileageInterval_mx) && ( actualMileage > theroyDnMax || actualMileage < theroyDnMin )) curList ++ List("2")
                  else if (angularSpeed > angularSpeedLimit && angle>angleLimit && avgSpeed > speedLimit) curList ++ List("3")
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
                  val spd: List[String] = List(preGPS(3), preGPS(8), curGps(3))
                  val mle: List[String] = List(actualMileage.formatted("%.2f"), theroyUpMin.formatted("%.2f"),theroyUpMax.formatted("%.2f"),theroyDnMin.formatted("%.2f"),theroyDnMax.formatted("%.2f"))
                  val oth: List[String] =  List(avgSpeed.toString, timeInterval.toString, angle.toString, st2)
                  val msg = (bsc ++ spd ++ mle ++ oth).mkString(",")
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
