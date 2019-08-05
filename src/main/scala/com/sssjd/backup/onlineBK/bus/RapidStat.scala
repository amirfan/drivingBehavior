package com.sssjd.backup.onlineBK.bus

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Milliseconds, Minutes}
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.expressions.Hour
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import redis.clients.jedis.Jedis

import scala.math._


object RapidStat {

  val kafkaConf = LoadConfig.getKafkaConfig()
  val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  case class Drive(jsy:String,lspeed:Double,lutc:String,longtime:Long ,llon:String,llat:String,EHstatus:String,orientation:String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("rapidStatStreaming")
      .master("local[*]")
      .config("spark.default.parallelism", 100)
      .config("spark.streaming.concurrentJobs", 10)
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.release.num.duration", 5)
      .config("spark.streaming.blockInterval", 10)
      .config("spark.locality.wait", 100)
      .config("spark.streaming.kafka.consumer.cache.enabled",false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val getDist = (lut1:Double,lat1:Double,lut2:Double,lat2:Double) =>{
      val rad:Double = 6378.137
      val lts = sin(pow(abs(lat1 - lat2)*Pi/360,2))
      val lns = sin(pow(abs(lut1 - lut2)*Pi/360,2))
      val ltc = cos(lat1 * Pi /180) * cos(lat2 * Pi / 180)
      val distance = 2 * asin(sqrt(lts + lns * ltc)) * rad
      distance * 1000
    }
    spark.udf.register("Dist", getDist)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val KafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaConf.get("brokers_bus").getOrElse().toString,
      "group.id" -> "rapidBusConsumerStat",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")


    val topics = List(kafkaConf.get("topic_bus_roadmatch").getOrElse().toString)

    val message: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, KafkaParams))

    val ds: DStream[String] = message.map(record =>record.value).window(Minutes(180),Minutes(3))

    import spark.implicits._

    val jedis: Jedis = getJedis()

    try{

    ds.foreachRDD(rdd =>{

      if( !rdd.isEmpty()){

        val df: DataFrame = rdd.map(_.split(",")).map(row =>{
          Drive(row(0),row(2).toDouble,row(3),row(4).toLong,row(5),row(6),row(7),row(8))
        }).toDF()

      df.createOrReplaceTempView("ori_roadmatch")

      spark.sql("select 'st' as id, count(*) as cnt from ori_roadmatch").createOrReplaceTempView("ori_cnt")

        //临时表：相邻时刻数据拉平
      val sql =
        s"""
           |select
           |jsy,
           |lag(llon,1,0) over(partition by jsy order by longtime) as prelon,
           |lag(llat,1,0) over(partition by jsy order by longtime) as prelat,
           |lag(longtime,1,0) over(partition by jsy order by longtime) as pretime,
           |lag(lspeed,1,0) over(partition by jsy order by longtime) as prespeed,
           |lag(orientation,1,0) over(partition by jsy order by longtime) as preorien,
           |llon,
           |llat,
           |longtime,
           |lspeed,
           |orientation
           |from ori_roadmatch
           |where lspeed<=80
           |having prelat <> 0 and prespeed <=80
       """.stripMargin
      spark.sql(sql).createOrReplaceTempView("ori_detail")

        // 计算加速度/方向角/角速度/里程差
      val sql_2 =
        s"""
           |select
           |tab.* ,
           |tab.turn/((longtime-pretime)/1000) as aturn,
           |abs(actualDistance-theroyDistance) as distance
           |from(
           |select
           |jsy,prelat,prelon,pretime,prespeed,preorien,llat,llon,longtime,lspeed,orientation,
           |((lspeed - prespeed)/3.6)/((longtime-pretime)/1000) as aspeed,
           |case when
           |abs(orientation-preorien)>180 then 360-(abs(orientation-preorien))
           |else abs(orientation-preorien) end as turn,
           |(prespeed + lspeed)/7.2 * ((longtime-pretime)/1000) as theroyDistance,
           |Dist( prelon, prelat, llon, llat) as actualDistance
           |from ori_detail) tab
       """.stripMargin
      spark.sql(sql_2).createOrReplaceTempView("ori_stat")


      //计算里程差均值标准差
      //计算急加均值标准差
      val sql_up =
        s"""
           |select
           |'st' as id,
           |avg(aspeed) as avg_upaspeed,
           |stddev(aspeed) as std_upaspeed,
           |min(aspeed) as mi_upaspeed,
           |max(aspeed) as mx_upaspeed,
           |avg(distance) as avg_updist,
           |stddev(distance) as std_updist,
           |min(distance) as mi_updist,
           |max(distance) as mx_updist
           |from ori_stat
           |where aspeed > 0
         """.stripMargin

      spark.sql(sql_up).createOrReplaceTempView("stat_up")

      //计算里程差均值标准差
      //计算急减均值标准差
      val sql_dn =
        s"""
           |select
           |'st' as id,
           |avg(aspeed) as avg_dnaspeed,
           |stddev(aspeed) as std_dnaspeed,
           |min(aspeed) as mi_dnaspeed,
           |max(aspeed) as mx_dnaspeed,
           |avg(distance) as avg_dndist,
           |stddev(distance) as std_dndist,
           |min(distance) as mi_dndist,
           |max(distance) as mx_dndist
           |from ori_stat
           |where aspeed < 0
         """.stripMargin
      spark.sql(sql_dn).createOrReplaceTempView("stat_dn")

      //计算转弯均值标准差
      //计算角速度均值标准差
      //计算平均速度均值标准差
      val sql_tn =
        s"""
           |select
           |'st' as id,
           |avg(turn) as avg_turn,
           |stddev(turn) as std_turn,
           |min(turn) as mi_turn,
           |max(turn) as mx_turn,
           |avg(aturn) as avg_aturn,
           |stddev(aturn) as std_aturn,
           |min(aturn) as mi_aturn,
           |max(aturn) as mx_aturn,
           |avg((lspeed + prespeed)/2) as avg_tnspeed,
           |stddev((lspeed + prespeed)/2) as std_tnspeed,
           |min((lspeed + prespeed)/2) as mi_tnspeed,
           |max((lspeed + prespeed)/2) as mx_tnspeed
           |from ori_stat
           |where turn > 60
         """.stripMargin
      spark.sql(sql_tn).createOrReplaceTempView("stat_tn")

      //关联各种统计值
//      val sql_st =
//        s"""
//           |select
//           |DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') as time,
//           |cnt,
//           |avg_upaspeed,std_upaspeed,mi_upaspeed,mx_upaspeed,avg_updist,std_updist,mi_updist,mx_updist,
//           |avg_dnaspeed,std_dnaspeed,mi_dnaspeed,mx_dnaspeed,avg_dndist,std_dndist,mi_dndist,mx_dndist,
//           |avg_turn,std_turn,mi_turn,mx_turn,avg_aturn,std_aturn,mi_aturn,mx_aturn,avg_tnspeed,std_tnspeed,mi_tnspeed,mx_tnspeed
//           |from stat_up up
//           |join stat_dn dn on up.id = dn.id
//           |join stat_tn tn on up.id = tn.id
//           |join ori_cnt cn on up.id = cn.id
//         """.stripMargin

        val sql_st =
          s"""
             |select
             |DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') as time,
             |cnt,
             |avg_upaspeed,std_upaspeed,mi_upaspeed,mx_upaspeed,avg_updist,std_updist,mi_updist,mx_updist,
             |avg_dnaspeed,std_dnaspeed,mi_dnaspeed,mx_dnaspeed,avg_dndist,std_dndist,mi_dndist,mx_dndist,
             |avg_turn,std_turn,mi_turn,mx_turn,avg_aturn,std_aturn,mi_aturn,mx_aturn,avg_tnspeed,std_tnspeed,mi_tnspeed,mx_tnspeed
             |from ori_cnt cn
             |join stat_up up on cn.id = up.id
             |join stat_dn dn on cn.id = dn.id
             |join stat_tn tn on cn.id = tn.id

         """.stripMargin

        var jstat:String = null

        try{
        jstat = spark.sql(sql_st).toJSON.first()
        }
        catch {
          case e:Exception =>Thread.currentThread().interrupt()
        }



      println(jstat)
      if (jstat.contains("avg_upaspeed") && jstat.contains("avg_updist") && jstat.contains("avg_dnaspeed") && jstat.contains("avg_dndist") && jstat.contains("avg_turn") && jstat.contains("avg_aturn") && jstat.contains("avg_tnspeed"))
        jedis.hset("rapidStat","stat",jstat)
      }
    })

    }
    catch{
     case e:Exception => e.printStackTrace()
    }
    finally retJedis(jedis)

    ssc.start()
    ssc.awaitTermination()
  }

 }