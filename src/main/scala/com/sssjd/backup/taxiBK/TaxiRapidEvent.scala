package com.sssjd.backup.taxiBK

import java.text.SimpleDateFormat
import java.util.Date
import com.sssjd.utils.UDFDistance._
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import com.sssjd.utils.{HbaseUtil, SqlserverUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer
import scala.math.abs

object TaxiRapidEvent extends EventStatus {

  val acceThreshold = 0.414
  val deceThreshold = -0.407
  val mileageInterval_mi = 26.256
  val mileageInterval_mx = 41.484
  val speedLimit = 46.62 // --km/h
  val angleLimit = 45
  val angularSpeedLimit = 20.486
  val table = "T_AlarmJ"
  val hbaseTable = "taxi_ns:rapidDetails"

  val kafkaConf = LoadConfig.getKafkaConfig()


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TaxiEventStreaming")
      .master("local[*]")
      .config("spark.default.parallelism", 1000)
      .config("spark.streaming.concurrentJobs", 10)
//      .config("spark.scheduler.mode", "FAIR")
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
      "group.id" -> "rapidTaxiEvent",
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
    * 急加速/急减速/急转弯
    * * preGPS(pptime,pplut,pplat,ppspeed,ppturn,ptime,plut,plat,pspeed,pturn)
    * * curGps(time,lut,lat,speed,turn)
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
            val curGps = List(curList(4), curList(5), curList(6), curList(2), curList(7))
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

              if (timeInterval >= 5 && timeInterval <= 60 && curGps(3).toFloat<120) {
                //急加急减条件条件：加速度大于阈值 && 里程差大于/小于临界值 && 里程大于/小于临界值
                //急转弯条件：角速度大于阈值 && 角度大于阈值 && 速度大于阈值
                var stat =
                  if (sudSpeed >acceThreshold && (mileageInterval>mileageInterval_mi && mileageInterval<mileageInterval_mx) && ( actualMileage > theroyUpMax || actualMileage < theroyUpMin ))
                    curList ++ List("1")
                  else if (sudSpeed < deceThreshold && (mileageInterval>mileageInterval_mi && mileageInterval<mileageInterval_mx) && ( actualMileage > theroyDnMax || actualMileage < theroyDnMin ))
                    curList ++ List("2")
                  else if (angularSpeed > angularSpeedLimit && angle>angleLimit && avgSpeed > speedLimit)
                    curList ++ List("5")
                  else curList ++ List("0")

                val st = stat.takeRight(1).mkString("")

                //事件判断
                if (!st.equals("0")) {
                  println(stat)
                  val stat2 = eventStatus(st, jsy, preGPS, curGps, curList, jedis)
                  if(stat2.nonEmpty){
                    val sql =
                      s"""
                        |insert into
                        |$table(dbuscard,dguid,starttime,endtime,conTime,alarmtype,updateTime,
                        |alarmLevel,deal,uid,tips)
                        |values(?,?,?,?,?,?,?,0,0,newid(),?)
                      """.stripMargin

                    val dbuscard = stat2.getOrElse("dbuscard","未知")
                    val dguid = stat2.getOrElse("dguid","未知")
                    val stime = stat2.getOrElse("starttime","946656000000").toLong
                    val starttime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(stime)
                    val etime = stat2.getOrElse("endtime","946656000000").toLong
                    val endtime:String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(etime)
                    val alarmtype = stat2.getOrElse("alarmtype","未知")
                    val updatetime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
//                    val speed :String = avgSpeed.toString
                    val tips: String = "加速度值" +sudSpeed.formatted("%.2f") +";" + "预计行驶里程"+theryAvgMileage.formatted("%.2f") +";"+ "实际行驶里程"+actualMileage.formatted("%.2f")

                    val ary: Array[Any] = Array(dbuscard,dguid,starttime,endtime,180,alarmtype,updatetime,tips)
                    println(ary.mkString(";"))
                    SqlserverUtil.executeUpdate(sql,ary)
                  }
                }

                //统计明细写入hbase
                if (st != "0") {
                  val rowkey = jsy + "_" + curGps.head.reverse
                  val data = ArrayBuffer[(String,AnyRef)]()
                  data += (("dbuscard",stat(1)))
                  data += (("time",stat(3)))
                  data +=(("lut",stat(5)))
                  data += (("lat",stat(6)))
                  data +=(("ppspeed",preGPS(3)))
                  data +=(("pspeed",preGPS(8)))
                  data +=(("speed",curGps(3)))
                  data +=(("actualMileage",actualMileage.formatted("%.2f")))
                  data +=(("theryAvgMileage",theryAvgMileage.formatted("%.2f")))
                  data +=(("theroyUpMin",theroyUpMin.formatted("%.2f")))
                  data +=(("theroyUpMax",theroyUpMax.formatted("%.2f")))
                  data +=(("theroyDnMin",theroyDnMin.formatted("%.2f")))
                  data +=(("theroyDnMax",theroyDnMax.formatted("%.2f")))
                  data +=(("timeInterval",timeInterval.toString))
                  data +=(("angle",angle.toString))
                  data +=(("status",st))

                  println(data.mkString(";"))

                  val hbaseClient = HbaseUtil.getInstance()
                  hbaseClient.init(hbaseTable)
                  hbaseClient.put(rowkey,"cf",data)
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
