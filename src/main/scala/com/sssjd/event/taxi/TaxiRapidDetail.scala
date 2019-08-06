package com.sssjd.event.taxi

import java.text.SimpleDateFormat
import java.util.Date

import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import com.sssjd.utils.UDFDistance._
import com.sssjd.utils.{HbaseUtil, JsonUtil, SqlserverUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer
import scala.math.abs

object TaxiRapidDetail {

  private val table = "T_AlarmJ"
  private val hbaseTable = "taxi_ns:rapidDetails"
  private val kafkaConf = LoadConfig.getKafkaConfig()
  private val sqlserverConf = LoadConfig.getSqlServerConfig()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TaxiEventStreaming")
      .master("local[*]")
      .config("spark.default.parallelism", 1000)
      .config("spark.streaming.concurrentJobs", 10)
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
        jedis = getJedis()
        val jstat = jedis.hget("rapidStatTaxi","stat")
        val js = JsonUtil.getObjectFromJson(jstat)
        //急加阈值
        val avg_upaspeed = js.get("avg_upaspeed").toString.toDouble //急加速度平均值
        val std_upaspeed = js.get("std_upaspeed").toString.toDouble //急加速度标准差
        val upThreshold = avg_upaspeed + 3 * std_upaspeed          //计算加速度阈值(基于均值标准差确定)
        val upTdBackup = js.get("iqr_upaspeed_max").toString.toDouble //加速度阈值(基于加速度Q3+3IQR确定--待用)
        val upMileageInterval_mi = js.get("iqr_updist").toString.toDouble //里程差最小值(急加里程差Q3+1.5IQR)
        val upMileageInterval_mx = js.get("iqr_updist_max").toString.toDouble //里程差最大值(急加里程差Q3+3IQR)
        //急减阈值
        val avg_dnaspeed = js.get("avg_dnaspeed").toString.toDouble //急减速度平均值
        val std_dnaspeed = js.get("std_dnaspeed").toString.toDouble //急减速度标准差
        val dnThreshold = avg_dnaspeed - 3 * std_upaspeed         //计算加速度阈值(基于均值标准差确定)
        val dnTdBackup = js.get("iqr_dnaspeed_max").toString.toDouble //加速度阈值(基于加速度Q3+3IQR确定--待用)
        val dnMileageInterval_mi = js.get("iqr_dndist").toString.toDouble //里程差最小值(急减里程差Q3+1.5IQR)
        val dnMileageInterval_mx = js.get("iqr_dndist_max").toString.toDouble //里程差最大值(急减里程差Q3+3IQR)
        //急转阈值
        val turnThreshold = js.get("avg_turn").toString.toDouble //转弯角度
        val avg_aturn = js.get("avg_aturn").toString.toDouble //角速度平均值
        val std_aturn = js.get("std_aturn").toString.toDouble //角速度标准差
        val angularSpeedThreshold = avg_aturn + 3 * std_aturn   //角速度阈值(基于均值标准差确定)
        val angularSpeedThresholdBackup = js.get("iqr_aturn_max").toString.toDouble //角速度阈值(基于角速度Q3+3IQR确定--待用)
        val avg_tnspeed = js.get("avg_tnspeed").toString.toDouble //转弯速度平均值
        val std_tnspeed = js.get("std_tnspeed").toString.toDouble //转弯速度标准差
        val avgSpeedThreshold = avg_tnspeed + 3 * std_tnspeed //速度阈值(基于均值标准差确定)
        val avgSpeedThresholdBackup = js.get("iqr_tnspeed").toString.toDouble //角速度阈值(基于角速度Q3+1.5IQR确定--待用)

        try {
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
                  if (sudSpeed >upThreshold && (mileageInterval>upMileageInterval_mi && mileageInterval<upMileageInterval_mx) && ( actualMileage > theroyUpMax || actualMileage < theroyUpMin ) && angle<30)
                    curList ++ List("1")
                  else if (sudSpeed < dnThreshold && (mileageInterval>dnMileageInterval_mi && mileageInterval<dnMileageInterval_mx) && ( actualMileage > theroyDnMax || actualMileage < theroyDnMin ) && angle<30)
                    curList ++ List("2")
                  else if (angularSpeed > angularSpeedThreshold && angle>turnThreshold && avgSpeed > avgSpeedThreshold)
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
                    .concat("(标准值").concat(upThreshold.formatted("%.3f"))
                    .concat("); 正常行驶里程范围[")
                    .concat(theroyUpMin.formatted("%.2f"))
                    .concat("m - ")
                    .concat(theroyUpMax.formatted("%.2f"))
                    .concat("m]; 实际行驶里程").concat(actualMileage.formatted("%.2f"))
                    .concat("m")
                  val dntips: String = "加速度值" +sudSpeed.formatted("%.2f")
                     .concat("(标准值").concat(dnThreshold.formatted("%.3f"))
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
                    val jdbcDriver = sqlserverConf.get("jdbcDriver").getOrElse().toString
                    val jdbcSize = sqlserverConf.get("jdbcSize").getOrElse().toString.toInt
                    val connectionUrl = sqlserverConf.get("taxi_connectionUrl").getOrElse().toString
                    SqlserverUtil(jdbcDriver,jdbcSize,connectionUrl).executeUpdate(sql,ary)
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
