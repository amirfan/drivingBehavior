package com.sssjd.backup.onlineBK.taxiQuantile

import java.util.Properties

import com.sssjd.configure.LoadConfig
import com.sssjd.utils.JsonUtil
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.math._

object RapidQuantile extends EventStatus {

//  val up_numThreshold = 2 // 急加速度 avg_upaspeed + up_numThreshold * std_upaspeed
//  val dn_numThreshold = 2 // 急减速度 avg_dnaspeed - dn_numThreshold * std_dnaspeed
//  val tn_numThreshold = 1 // 角速度   avg_aturn + tn_numThreshold * std_aturn
//  val sp_numThreshold = 1 // 转弯速度 avg_tnspeed + sp_numThreshold * std_tnspeed
//  val ag_numThreahold = 0 // 转弯角度 avg_turn + ag_numThreahold * std_turn
//  val ml_numThreahold = 1 // 里程差  avg_updist + ml_numThreahold * std_updist / avg_dndist + ml_numThreahold * std_dndist

  val kafkaConf = LoadConfig.getKafkaConfig()


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("rapidStreamingTaxi2")
      .master("local[*]")
      .config("spark.default.parallelism", 100)
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
      "bootstrap.servers" -> kafkaConf.get("brokers_taxi").getOrElse().toString,
      "group.id" -> "rapidTaxiConsumer",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics = List(kafkaConf.get("topic_taxi_roadmatch").getOrElse().toString)

    println(kafkaConf.get("brokers_taxi").getOrElse().toString)

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
    * taxiPreGps存储上一个GPS信息 K(jsy)->V(时间,速度,角度) ---------(时间,速度,经度,纬度,角度)
    * taxiPreStatus记录上一时刻三急状态(0/1/2/3)
    * taxiRapidDetail记录评分区间三急数目
    *
    */

  def fun(records: RDD[ConsumerRecord[String, String]]): Unit = {

    if (!records.isEmpty()) {

      records.foreachPartition { partitionRDD =>

        val jedis: Jedis = getJedis()

        val jstat = jedis.hget("rapidStatTaxi","stat")
        val js = JsonUtil.getObjectFromJson(jstat)
        val iqr_upaspeed = js.get("iqr_upaspeed").toString.toDouble //急加速度Q3+1.5IQR
        val iqr_upaspeed_max = js.get("iqr_upaspeed_max").toString.toDouble //急加速度Q3+3IQR
        val iqr_upgap = js.get("iqr_upgap").toString.toDouble //急加平均速度差Q3+1.5IQR
        val iqr_upgap_max = js.get("iqr_upgap_max").toString.toDouble //急加平均速度差Q3+3IQR
        val iqr_updist = js.get("iqr_updist").toString.toDouble //急加里程差Q3+1.5IQR
        val iqr_updist_max = js.get("iqr_updist_max").toString.toDouble //急加里程差Q3+3IQR


        val iqr_dnaspeed = js.get("iqr_dnaspeed").toString.toDouble //急减速度Q3+1.5IQR
        val iqr_dnaspeed_max = js.get("iqr_dnaspeed_max").toString.toDouble //急减速度Q3+3IQR
        val iqr_dngap = js.get("iqr_dngap").toString.toDouble //急减平均速度差Q3+1.5IQR
        val iqr_dngap_max = js.get("iqr_dngap_max").toString.toDouble //急减平均速度差Q3+3IQR
        val iqr_dndist = js.get("iqr_dndist").toString.toDouble //急减里程差Q3+1.5IQR
        val iqr_dndist_max = js.get("iqr_dndist_max").toString.toDouble //急减里程差Q3+3IQR


        val iqr_turn = js.get("iqr_turn").toString.toDouble //转弯角度Q3+1.5IQR
        val iqr_turn_max = js.get("iqr_turn_max").toString.toDouble //转弯角度Q3+3IQR
        val iqr_aturn = js.get("iqr_aturn").toString.toDouble //转弯角速度Q3+1.5IQR
        val iqr_aturn_max = js.get("iqr_aturn_max").toString.toDouble //转弯角速度Q3+1.5IQR
        val iqr_tnspeed = js.get("iqr_tnspeed").toString.toDouble //转弯速度Q3+1.5IQR
        val iqr_tnspeed_max = js.get("iqr_tnspeed_max").toString.toDouble //转弯速度Q3+3IQR


        try {
          partitionRDD.foreach(row => {

            val curList = row.value().split(",").toList

            val jsy = curList.head

            val curGps = List(curList(4), curList(5), curList(6), curList(2), curList(8))

            var preGPS = List[String]()
            val preString = jedis.hget("taxiPreGps", jsy)

            if (preString == null) {
              jedis.hset("taxiPreGps", jsy, curGps.mkString(","))
            }
            else {
              preGPS = preString.split(",").toList
              val gps = preGPS.takeRight(5) ++ curGps
              jedis.hset("taxiPreGps", jsy, gps.mkString(","))
            }

            /**
              * 4000368,苏L02208D,21.0,2019-05-17 13:45:51.0,1558071951,119.170966,31.954823,3,278.0
              * curList(jsy,carID,speed,tm,time,lut,lat,eh,turn,...)
              * (pptime,ppspeed,ppturn,ptime,pspeed,pturn)
              * **************************************************************************
              * preGPS(pptime,pplut,pplat,ppspeed,ppturn,ptime,plut,plat,pspeed,pturn)
              * curGps(time,lut,lat,speed,turn)
              */

            if (preGPS.length == 10 && preGPS.head < preGPS(5) && preGPS(5) < curGps.head) {

              val timeInterval: Long = (curGps.head.toLong - preGPS(5).toLong) / 1000 // 时间间隔:s

              //加速度(m/s^2)
              val sudSpeed = (curGps(3).toFloat - preGPS(8).toFloat) / (timeInterval * 3.6) //加速度m/s^2

              //里程差(m)
              val avgSpeed = (curGps(3).toFloat + preGPS(8).toFloat) / 2 //理论平均速度km/h
              val theroyDistance = avgSpeed * timeInterval / 3.6 //理论里程
              val actualDistance = getDist(preGPS(6).toDouble, preGPS(7).toDouble, curGps(1).toDouble, curGps(2).toDouble) //实际行驶里程
              val actualSpeed = actualDistance / timeInterval
              val distance = abs(actualDistance - theroyDistance)
              //里程差
              val speedInterval = abs(actualSpeed - avgSpeed) //平均速度差


              //角速度/转弯角度/平均速度(km/h)
              val angTmp = abs(curGps(4).toFloat - preGPS(9).toFloat)
              val angle = angTmp match { //  角度变化
                case _ if angTmp > 180.0 => 360.0 - angTmp
                case _ => angTmp
              }
              val angSpeed = angle / timeInterval // 角速度

              //条件1：加速度大于阈值&&里程差大于阈值||加速度大于阈值&&速度大于阈值||里程差大于阈值&&时间差小于阈值
              //条件2：角速度大于阈值&&角度大于阈值&&速度大于阈值

              var stat: List[String] =
                if (sudSpeed > iqr_upaspeed && distance > iqr_updist && speedInterval > iqr_upgap && timeInterval <= 30) curList ++ List("1")
                else if (sudSpeed < iqr_dnaspeed && distance > iqr_dndist && speedInterval > iqr_dngap && timeInterval <= 30) curList ++ List("2")
                else if (angSpeed > iqr_aturn && angle > iqr_turn && avgSpeed > iqr_tnspeed) curList ++ List("3")
                else curList ++ List("0")

              //过滤连续状态
              val st = stat.takeRight(1).mkString("")
              val status = jedis.hget("taxiPreStatus", jsy)
              if (st == status) stat = curList ++ List("0")
              else jedis.hset("taxiPreStatus", jsy, st)

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
                var mse = jedis.hget("taxiRapidDetail", jsy)
                if (mse == null) {
                  mse = st2
                } else {
                  mse = mse + ":" + st2
                }
                jedis.hset("taxiRapidDetail", jsy, mse)

                //发送kafka(jsy,cph,tm,lut,lat,speed,preSpeed,theroySpeed,actualSpeed,theroyMile,actualMile,tmInterval,angle,st)
                val bsc: List[String] = List(stat.head, stat(1), stat(3), stat(5), stat(6))
                val spd: List[String] = List(preGPS(3), preGPS(8), stat(2))
                val mle: List[String] = List(theroyDistance.formatted("%.2f"), actualDistance.formatted("%.2f"), avgSpeed.toString, actualSpeed.formatted("%.2f"), timeInterval.toString, angle.toString, st2)
                val msg = (bsc ++ spd ++ mle).mkString(",")
                println(msg)
                detailsProducer(msg)
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
