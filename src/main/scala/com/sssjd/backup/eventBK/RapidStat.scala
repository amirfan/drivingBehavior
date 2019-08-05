package com.sssjd.backup.eventBK

import java.text.SimpleDateFormat

import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.math._

object RapidStat {

  val kafkaConf = LoadConfig.getKafkaConfig()
  val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  case class Drive(jsy:String,lspeed:Double,lutc:String,longtime:Long ,llon:String,llat:String,EHstatus:String,orientation:String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
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
      "bootstrap.servers" -> kafkaConf.get("brokers_taxi").getOrElse().toString,
      "group.id" -> "rapidTaxiConsumerStat2",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")


    val topics = List(kafkaConf.get("topic_taxi_roadmatch").getOrElse().toString)

    val message: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, KafkaParams))

    val ds: DStream[String] = message.map(record =>record.value).window(Minutes(60),Minutes(1))

    import spark.implicits._

    var jedis:Jedis = null

    try{
      jedis = getJedis()

    ds.foreachRDD(rdd =>{

      if( !rdd.isEmpty()) {

        val df: DataFrame = rdd.map(_.split(",")).map(row => {
           Drive(row(0), row(2).toDouble, row(3), row(4).toLong, row(5), row(6), row(7), row(8))
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
             |abs(actualDistance-theroyDistance) as distance,
             |actualDistance/((longtime-pretime)/1000) as actualAverageSpeed,
             |(lspeed - prespeed)/7.2 as theroyAverageSpeed,
             |abs(actualDistance/((longtime-pretime)/1000) - (lspeed - prespeed)/7.2) as speedGap
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
        //计算平均速度差均值标准差
        val sql_up =
        s"""
           |select
           |'st' as id,
           |avg(aspeed) as avg_upaspeed,
           |stddev(aspeed) as std_upaspeed,
           |min(aspeed) as mi_upaspeed,
           |percentile_approx(aspeed,array(0.25))[0] as approx25_upaspeed,
           |percentile_approx(aspeed,array(0.5))[0] as median_upaspeed,
           |percentile_approx(aspeed,array(0.75))[0] as approx75_upaspeed,
           |max(aspeed) as mx_upaspeed,
           |avg(distance) as avg_updist,
           |stddev(distance) as std_updist,
           |min(distance) as mi_updist,
           |percentile_approx(distance,array(0.25))[0] as approx25_updist,
           |percentile_approx(distance,array(0.5))[0] as median_updist,
           |percentile_approx(distance,array(0.75))[0] as approx75_updist,
           |max(distance) as mx_updist,
           |avg(speedGap) as avg_upgap,
           |stddev(speedGap) as std_upgap,
           |min(speedGap) as mi_upgap,
           |percentile_approx(speedGap,array(0.25))[0] as approx25_upgap,
           |percentile_approx(speedGap,array(0.5))[0] as median_upgap,
           |percentile_approx(speedGap,array(0.75))[0] as approx75_upgap,
           |max(speedGap) as mx_upgap
           |from ori_stat
           |where aspeed > 0
         """.stripMargin

        spark.sql(sql_up).createOrReplaceTempView("stat_up")

        //计算里程差均值标准差
        //计算急减均值标准差
        //计算平均速度差均值标准差
        val sql_dn =
        s"""
           |select
           |'st' as id,
           |avg(aspeed) as avg_dnaspeed,
           |stddev(aspeed) as std_dnaspeed,
           |min(aspeed) as mi_dnaspeed,
           |percentile_approx(aspeed,array(0.25))[0] as approx25_dnaspeed,
           |percentile_approx(aspeed,array(0.5))[0] as median_dnaspeed,
           |percentile_approx(aspeed,array(0.75))[0] as approx75_dnaspeed,
           |max(aspeed) as mx_dnaspeed,
           |avg(distance) as avg_dndist,
           |stddev(distance) as std_dndist,
           |min(distance) as mi_dndist,
           |percentile_approx(distance,array(0.25))[0] as approx25_dndist,
           |percentile_approx(distance,array(0.5))[0] as median_dndist,
           |percentile_approx(distance,array(0.75))[0] as approx75_dndist,
           |max(distance) as mx_dndist,
           |avg(speedGap) as avg_dngap,
           |stddev(speedGap) as std_dngap,
           |min(speedGap) as mi_dngap,
           |percentile_approx(speedGap,array(0.25))[0] as approx25_dngap,
           |percentile_approx(speedGap,array(0.5))[0] as median_dngap,
           |percentile_approx(speedGap,array(0.75))[0] as approx75_dngap,
           |max(speedGap) as mx_dngap
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
           |percentile_approx(turn,array(0.25))[0] as approx25_turn,
           |percentile_approx(turn,array(0.5))[0] as median_turn,
           |percentile_approx(turn,array(0.75))[0] as approx75_turn,
           |max(turn) as mx_turn,
           |avg(aturn) as avg_aturn,
           |stddev(aturn) as std_aturn,
           |min(aturn) as mi_aturn,
           |percentile_approx(aturn,array(0.25))[0] as approx25_aturn,
           |percentile_approx(aturn,array(0.5))[0] as median_aturn,
           |percentile_approx(aturn,array(0.75))[0] as approx75_aturn,
           |max(aturn) as mx_aturn,
           |avg((lspeed + prespeed)/2) as avg_tnspeed,
           |stddev((lspeed + prespeed)/2) as std_tnspeed,
           |min((lspeed + prespeed)/2) as mi_tnspeed,
           |percentile_approx((lspeed + prespeed)/2,array(0.25))[0] as approx25_tnspeed,
           |percentile_approx((lspeed + prespeed)/2,array(0.5))[0] as median_tnspeed,
           |percentile_approx((lspeed + prespeed)/2,array(0.75))[0] as approx75_tnspeed,
           |max((lspeed + prespeed)/2) as mx_tnspeed
           |from ori_stat
           |where turn > 0
         """.stripMargin
        spark.sql(sql_tn).createOrReplaceTempView("stat_tn")

        //关联各种统计值
        /**
          * IQR=Q3-Q1(内距)
          * 上限(非异常范围内的最大值)=Q3+1.5IQR
          * 下限(非异常范围内的最小值)=Q1-1.5IQR
          * 离群值>Q3+1.5IQR
          * 极端值>Q3+3IQR
          */

        val sql_st =
          s"""
             |select
             |DATE_FORMAT(NOW(),'yyyy-MM-dd HH:mm:ss') as time,
             |cnt,
             |avg_upaspeed,std_upaspeed,mi_upaspeed,approx25_upaspeed,median_upaspeed,approx75_upaspeed,mx_upaspeed,
             |approx75_upaspeed+1.5*(approx75_upaspeed-approx25_upaspeed) as iqr_upaspeed,
             |approx75_upaspeed+3*(approx75_upaspeed-approx25_upaspeed) as iqr_upaspeed_max,
             |avg_updist,std_updist,mi_updist,approx25_updist,median_updist,approx75_updist,mx_updist,
             |approx75_updist+1.5*(approx75_updist-approx25_updist) as iqr_updist,
             |approx75_updist+3*(approx75_updist-approx25_updist) as iqr_updist_max,
             |avg_upgap,std_upgap,mi_upgap,approx25_upgap,median_upgap,approx75_upgap,mx_upgap,
             |approx75_upgap+1.5*(approx75_upgap-approx25_upgap) as iqr_upgap,
             |approx75_upgap+3*(approx75_upgap-approx25_upgap) as iqr_upgap_max,
             |avg_dnaspeed,std_dnaspeed,mi_dnaspeed,approx25_dnaspeed,median_dnaspeed,approx75_dnaspeed,mx_dnaspeed,
             |approx25_dnaspeed-1.5*(approx75_dnaspeed-approx25_dnaspeed) as iqr_dnaspeed,
             |approx25_dnaspeed-3*(approx75_dnaspeed-approx25_dnaspeed) as iqr_dnaspeed_max,
             |avg_dndist,std_dndist,mi_dndist,approx25_dndist,median_dndist,approx75_dndist,mx_dndist,
             |approx75_dndist+1.5*(approx75_dndist-approx25_dndist) as iqr_dndist,
             |approx75_dndist+3*(approx75_dndist-approx25_dndist) as iqr_dndist_max,
             |avg_dngap,std_dngap,mi_dngap,approx25_dngap,median_dngap,approx75_dngap,mx_dngap,
             |approx75_dngap+1.5*(approx75_dngap-approx25_dngap) as iqr_dngap,
             |approx75_dngap+3*(approx75_dngap-approx25_dngap) as iqr_dngap_max,
             |avg_turn,std_turn,mi_turn,approx25_turn,median_turn,approx75_turn,mx_turn,
             |approx75_turn+1.5*(approx75_turn-approx25_turn) as iqr_turn,
             |approx75_turn+3*(approx75_turn-approx25_turn) as iqr_turn_max,
             |avg_aturn,std_aturn,mi_aturn,approx25_aturn,median_aturn,approx75_aturn,mx_aturn,
             |approx75_aturn+1.5*(approx75_aturn-approx25_aturn) as iqr_aturn,
             |approx75_aturn+3*(approx75_aturn-approx25_aturn) as iqr_aturn_max,
             |avg_tnspeed,std_tnspeed,mi_tnspeed,approx25_tnspeed,median_tnspeed,approx75_tnspeed,mx_tnspeed,
             |approx75_tnspeed+1.5*(approx75_tnspeed-approx25_tnspeed) as iqr_tnspeed,
             |approx75_tnspeed+3*(approx75_tnspeed-approx25_tnspeed) as iqr_tnspeed_max
             |from stat_up up
             |join stat_dn dn on up.id = dn.id
             |join stat_tn tn on up.id = tn.id
             |join ori_cnt cn on up.id = cn.id
         """.stripMargin

        val stat: DataFrame = spark.sql(sql_st)

        var jstat: String = null
        try {
          jstat = spark.sql(sql_st).toJSON.first()
        }
        catch {
          case e: Exception => Thread.currentThread().interrupt()
        }

        if (jstat.split(",").length == 83) {
          jedis.hset("rapidStatTaxi", "stat", jstat)
        }

        println(jstat)
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
