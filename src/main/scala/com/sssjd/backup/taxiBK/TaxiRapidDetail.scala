package com.sssjd.backup.taxiBK

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import com.sssjd.utils.UDFDistance._
import com.sssjd.utils.{HbaseUtil, SqlserverUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer
import scala.math.abs

object TaxiRapidDetail extends EventStatus {

  val acceThreshold = 0.414
  val deceThreshold = -0.407

//  val acceThreshold = 0.885
//  val deceThreshold = -0.878
//  val mileageInterval_mi = 26.256
//  val mileageInterval_mx = 41.484
  val mileageInterval_mi = 26.256
  val mileageInterval_mx = 100
  val speedLimit = 33.79 // --km/h
  val angleLimit = 45
  val angularSpeedLimit = 20.486
  val table = "T_AlarmJ"
  val hbaseTable = "taxi_ns:rapidDetails"

  val kafkaConf = LoadConfig.getKafkaConfig()

  def getProPerties() = {
    val properties: Properties = new Properties()
    properties.load(this.getClass().getClassLoader().getResourceAsStream("sqlserver.properties"))
    properties
  }

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
                  if (sudSpeed >acceThreshold && (mileageInterval>mileageInterval_mi && mileageInterval<mileageInterval_mx) && ( actualMileage > theroyUpMax || actualMileage < theroyUpMin ) && angle<angleLimit)
                    curList ++ List("1")
                  else if (sudSpeed < deceThreshold && (mileageInterval>mileageInterval_mi && mileageInterval<mileageInterval_mx) && ( actualMileage > theroyDnMax || actualMileage < theroyDnMin ) && angle<angleLimit)
                    curList ++ List("2")
                  else if (angularSpeed > angularSpeedLimit && angle>angleLimit && avgSpeed > speedLimit)
                    curList ++ List("5")
                  else curList ++ List("0")

                val st = stat.takeRight(1).mkString("")

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

                  val sql =
                    s"""
                       |insert into
                       |$table(dbuscard,dguid,starttime,endtime,conTime,alarmtype,updateTime,
                       |alarmLevel,uid,tips,startpos,endpos)
                       |values(?,?,?,?,?,?,?,0,newid(),?,?,?)
                      """.stripMargin

                  val dtMap: Map[String, AnyRef] = data.toMap

                  val uptips: String = "加速度值" +sudSpeed.formatted("%.2f")
                    .concat("(标准值").concat(acceThreshold.formatted("%.3f"))
                    .concat("); 正常行驶里程范围[")
                    .concat(theroyUpMin.formatted("%.2f"))
                    .concat("m - ")
                    .concat(theroyUpMax.formatted("%.2f"))
                    .concat("m]; 实际行驶里程").concat(actualMileage.formatted("%.2f"))
                    .concat("m")
                  val dntips: String = "加速度值" +sudSpeed.formatted("%.2f")
                     .concat("(标准值").concat(deceThreshold.formatted("%.3f"))
                     .concat("); 正常行驶里程范围[")
                     .concat(theroyDnMin.formatted("%.2f"))
                     .concat("m - ")
                     .concat(theroyDnMax.formatted("%.2f"))
                     .concat("m]; 实际行驶里程").concat(actualMileage.formatted("%.2f"))
                     .concat("m")

                  val dbuscard = dtMap.getOrElse("dbuscard","未知")
                  val dguid = jsy
                  val stime = preGPS(5).toLong
                  val starttime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(stime)
                  val etime = curGps.head.toLong
                  val endtime:String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(etime)
                  val alarmtype = dtMap.getOrElse("status","0")
                  val updatetime :String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

                  val tips = alarmtype match {
                    case "1" if actualMileage > 0 => uptips
                    case "2" if actualMileage > 0 => dntips
                    case "5" => "5"
                    case _ => null
                  }

                  val startpos = preGPS(6) +"," + preGPS(7)
                  val endpos = curGps(1) +"," + curGps(2)
                  if(tips != null){
                    val ary: Array[Any] = Array(dbuscard,dguid,starttime,endtime,timeInterval,alarmtype,updatetime,tips,startpos,endpos)
                    println(ary.mkString(";"))
                    val properties: Properties = getProPerties()
                    val url = properties.getProperty("taxi_connectionUrl")
                    SqlserverUtil(url).executeUpdate(sql,ary)
                  }

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
