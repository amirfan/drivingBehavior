package com.sssjd.offline.bus.stat

import java.text.SimpleDateFormat

import com.sssjd.backup.taxiBK.UDFDistance._
import com.sssjd.configure.LoadConfig
import com.sssjd.offline.bus.rapid.EventFrequency
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/***
  * 加速度为前后两点,添加里程判断
  */


object RapidSpeedJRStat extends EventFrequency {
//  val n_std_up = 2.6
//  val n_std_dn = 2.8
//  val n_std_speed = 0.25
//  val n_std_angle = 0.8

  val n_std_up = 0.5026947901583626
  val n_std_dn = -0.499124456277012
  val angleLimit = 20.486
  val speedLimit = 20
  val path = "hdfs://192.168.100.177:8020/user/root/VDP_bus/2017analyse/"
  val fm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  case class drive(DBusCard:String,LUTC:String,LLON:String,LLAT:String,LSpeed:String,LDir:String)


  System.setProperty("HADOOP_USER_NAME", LoadConfig.getHdfsUser())
  def main(args: Array[String]): Unit = {
    val fsPath = LoadConfig.getHdfsConfiguration()
    val spark: SparkSession = SparkSession.builder()
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "100")
      .config("spark.default.parallelism", "500")
      .appName("rapidOffline")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.udf.register( "Dist" , getDist )
    spark.udf.register( "theroyDistUpMin" , theroyDistUpMin )
    spark.udf.register( "theroyDistUpMax" , theroyDistUpMax )
    spark.udf.register( "theroyDistDnMin" , theroyDistDnMin )
    spark.udf.register( "theroyDistDnMax" , theroyDistDnMax )



      val dataSet: DataFrame = spark.read.option("header", "false").text("f:/JR/")

    import spark.implicits._
    val s = dataSet.rdd.map((row: Row) => {
      val r: Array[String] = row.toString().split(",")
      drive(r(1),r(2),r(3),r(4),r(5),r(6).replace("]",""))
    }).toDF()

    s.createOrReplaceTempView("s")

    val sql = s"""
       |select *,
       |unix_timestamp(LUTC) as lutclong
       |from s
     """.stripMargin
    val df = spark.sql(sql)

      val rapidDetail: DataFrame = calRapidSpeed(spark,df)
//      rapidDetail.repartition(1).write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv("f:/JR/")

//      val rapidDetailAndEvent = eventFrequency(spark,rapidDetail)
//      rapidDetailAndEvent.repartition(1).write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv("F:/JR_RES/")

  }

  def calRapidSpeed(spark: SparkSession,dataSet: DataFrame): DataFrame = {
    dataSet.createOrReplaceTempView("ori_roadmatch_pre")

    /**
      * 1过滤时间差在60s区间内数据
      * 2过滤速度80km/h区间内数据
      * 3速度转化为m/s
      */

    //临时表：相邻时刻数据拉平(jsy,经度1,纬度1,时间1,速度1,方向1,经度2,纬度2,时间2,速度2,方向2)
    val sql0 =
      s"""
         |select
         |DBusCard as jsy,
         |LUTC,
         |lutclong as longtime,
         |llon,
         |llat,
         |lspeed,
         |LDir as orientation
         |from ori_roadmatch_pre
       """.stripMargin
    spark.sql(sql0).createOrReplaceTempView("ori_roadmatch")


    val sql1 =
      s"""
         |select jsy,prelon,prelat,prelutc,pretime,
         |prespeed/3.6 as prespeed,
         |preorien,
         |llon,llat,LUTC,longtime,
         |lspeed/3.6 as lspeed,
         |(longtime - pretime) as timeInterval,
         |orientation
         |from(
         |select
         |jsy,
         |lag(llon,1,0) over(partition by jsy order by longtime) as prelon,
         |lag(llat,1,0) over(partition by jsy order by longtime) as prelat,
         |lag(LUTC,1,0) over(partition by jsy order by longtime) as prelutc,
         |lag(longtime,1,0) over(partition by jsy order by longtime) as pretime,
         |lag(lspeed,1,0) over(partition by jsy order by longtime) as prespeed,
         |lag(orientation,1,0) over(partition by jsy order by longtime) as preorien,
         |llon,
         |llat,
         |LUTC,
         |longtime,
         |lspeed,
         |orientation
         |from ori_roadmatch
         |having prelat <> 0 and lspeed<=80 and prespeed <=80  and (longtime - pretime)>=5 and (longtime - pretime)<=60) t
       """.stripMargin
    val tab_tmp1 = spark.sql(sql1)

    tab_tmp1.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tab_tmp1")

    /**
      * 计算加速度 and 里程
      * jsy,prelon,prelat,pretime,prespeed/3.6 as prespeed,preorien,llon,llat,longtime,lspeed/3.6 as lspeed,orientation
      */

    val sql2 =
      s"""
         |
         |select t.*,orient/timeInterval as angleSpeed
         |from (
         |select
         |*,
         |(lspeed - prespeed)/timeInterval as a_speed,
         |(lspeed + prespeed)/2  as avgSpeed,
         |case when
         |abs(orientation-preorien)>180 then 360-(abs(orientation-preorien))
         |else abs(orientation-preorien) end as orient,
         |Dist(prelon,prelat,llon,llat) as actualDist,
         |(lspeed + prespeed)/2 * timeInterval as avgDist,
         |theroyDistUpMin(prespeed,lspeed,timeInterval) as theroyDistUpMin,
         |theroyDistUpMax(prespeed,lspeed,timeInterval) as theroyDistUpMax,
         |theroyDistDnMin(prespeed,lspeed,timeInterval) as theroyDistDnMin,
         |theroyDistDnMax(prespeed,lspeed,timeInterval) as theroyDistDnMax
         |from tab_tmp1 ) t
       """.stripMargin
    spark.sql(sql2).createOrReplaceTempView("tab_tmp2")

    spark.sql(sql2).show()

    val s =
      s"""
         |select
         |avg_aspeed + 3 * std_aspeed as threshold_a,
         |approx75_gap + 1.5*(approx75_gap - approx25_gap) as threshold_gap_min,
         |approx75_gap + 3*(approx75_gap - approx25_gap) as threshold_gap_max,
         |avg_aspeed - 3 * std_aspeed as threshold_a_dn,
         |avg_gap,std_gap,mi_gap,approx25_gap,approx50_gap,approx75_gap,mx_gap,avg_aspeed,std_aspeed
         |from (
         |select
         |avg(a_speed) as avg_aspeed,
         |stddev(a_speed) as std_aspeed,
         |avg(abs(actualDist - avgDist)) as avg_gap,
         |min(abs(actualDist - avgDist)) as mi_gap,
         |percentile_approx(abs(actualDist - avgDist),array(0.25))[0] as approx25_gap,
         |percentile_approx(abs(actualDist - avgDist),array(0.50))[0] as approx50_gap,
         |percentile_approx(abs(actualDist - avgDist),array(0.75))[0] as approx75_gap,
         |max(abs(actualDist - avgDist)) as mx_gap,
         |stddev(abs(actualDist - avgDist)) as std_gap
         |from tab_tmp2 ) t
       """.stripMargin
    val threshold_a = spark.sql(s).first()(0).toString.toDouble
    val threshold_a2 = spark.sql(s).first()(3).toString.toDouble
    val threshold_gap_min = spark.sql(s).first()(1).toString.toDouble
    val threshold_gap_max = spark.sql(s).first()(2).toString.toDouble
    println(threshold_a,threshold_a2,threshold_gap_min,threshold_gap_max)
    spark.sql(s).show()

    /**
      * 计算急加急减速急转弯
      */

    val sql5 =
      s"""
         |select *,
         |case
         |when angleSpeed > $angleLimit and avgSpeed*3.6 > $speedLimit and orient>45 then 3
         |when a_speed > $n_std_up and (actualDist > theroyDistUpMax or actualDist < theroyDistUpMin) and abs(actualDist - avgDist) > $threshold_gap_min and abs(actualDist - avgDist) < $threshold_gap_max then 1
         |when a_speed < $n_std_dn and (actualDist > theroyDistDnMax or actualDist < theroyDistDnMin) and abs(actualDist - avgDist) > $threshold_gap_min and abs(actualDist - avgDist) < $threshold_gap_max then 2
         |else 0
         |end as rapidMark
         |from tab_tmp2
         |having rapidMark <> 0
       """.stripMargin

    spark.sql(sql5)

  }

}
