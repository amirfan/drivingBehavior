package com.sssjd.backup.eventBK

import com.sssjd.configure.LoadConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math._

object RapidSpeed {

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


    val getDist = (lut1:Double,lat1:Double,lut2:Double,lat2:Double) =>{
      val rad:Double = 6378.137
      val lts = sin(pow(abs(lat1 - lat2)*Pi/360,2))
      val lns = sin(pow(abs(lut1 - lut2)*Pi/360,2))
      val ltc = cos(lat1 * Pi /180) * cos(lat2 * Pi / 180)
      val distance = 2 * asin(sqrt(lts + lns * ltc)) * rad
      distance * 1000
    }
    spark.udf.register("Dist", getDist)

    val mon = 0
    val dataSet: DataFrame = spark.read.option("encoding", "utf8").option("header", "true").csv(fsPath+"roadmatch/"+mon)
    calRapidSpeed(spark,dataSet)
  }

  def calRapidSpeed(spark: SparkSession,dataSet: DataFrame) = {


    dataSet.createOrReplaceTempView("ori_roadmatch")



    spark.sql("select 'st' as id, count(*) as cnt from ori_roadmatch").createOrReplaceTempView("ori_cnt")

    //临时表：相邻时刻数据拉平(jsy,经度1,纬度1,时间1,速度1,方向1,经度2,纬度2,时间2,速度2,方向2)
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
         |where lspeed<=120
         |having prelat <> 0 and prespeed <=120 and (longtime - pretime)<=60
       """.stripMargin
    spark.sql(sql).createOrReplaceTempView("ori_detail")
    spark.sql(sql).show()

    val sql_2 =
      s"""
         |select *,
         |round(abs(actualSpeed - speed),2) as intervalSpeed
         |from (
         |select
         |jsy,
         |prelon,prelat,pretime,prespeed,preorien,
         |llon,llat,longtime,lspeed,orientation,
         |longtime - pretime as intervalTime,
         |round(Dist( prelon, prelat, llon, llat),2) as actualDistance,
         |round(Dist( prelon, prelat, llon, llat)/(longtime - pretime)*3.6,1) as actualSpeed,
         |round((prespeed + lspeed)/2,1) as speed
         |from ori_detail ) t
       """.stripMargin
    spark.sql(sql_2).createOrReplaceTempView("ori_stat")

    val ori_stat = spark.sql(sql_2)
//    ori_stat.repartition(1).write.partitionBy("jsy").mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv("hdfs://192.168.100.177:8020/user/root/VDP_bus/2017analyse/451/rapid_ori_stat/")



    //计算里程差均值标准差
    //计算急加均值标准差
    //计算平均速度差均值标准差
    val sql_up =
    s"""
       |select
       |jsy,
       |avg(intervalSpeed) as avg_upgap,
       |stddev(intervalSpeed) as std_upgap,
       |min(intervalSpeed) as mi_upgap,
       |percentile_approx(intervalSpeed,array(0.25))[0] as approx25_upgap,
       |percentile_approx(intervalSpeed,array(0.50))[0] as median_upgap,
       |percentile_approx(intervalSpeed,array(0.75))[0] as approx75_upgap,
       |percentile_approx(intervalSpeed,array(0.90))[0 ] as approx90_upgap,
       |percentile_approx(intervalSpeed,array(0.95))[0] as approx95_upgap,
       |max(intervalSpeed) as mx_upgap
       |from ori_stat
       |group by jsy
         """.stripMargin

    spark.sql(sql_up).createOrReplaceTempView("stat_up")
    spark.sql(sql_up).show()
//    val stat_up = spark.sql(sql_up)
//    stat_up.repartition(1).write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv("hdfs://192.168.100.177:8020/user/root/VDP_bus/2017analyse/451/rapid_stat_up/")

  }

}
