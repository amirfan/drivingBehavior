package com.sssjd.offline.taxi.rapid
import com.sssjd.utils.UDFDistance._
import com.sssjd.configure.LoadConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object RapidSpeedDZ extends EventFrequency {

  val n_std_up = 0.414
  val n_std_dn = -0.407
//  val n_std_dn = -0.878
  val angleSpeedLimit = 0.249
  val speedLimit = 47.422  //km/h
  val angleLimit = 1.985

  System.setProperty("HADOOP_USER_NAME", LoadConfig.getHdfsUser())

  def main(args: Array[String]): Unit = {
    val fsPath = LoadConfig.getHdfsTaxiCongig()
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

    for(mon <- Array(2) ){
      val dataSet: DataFrame = spark.read.option("encoding", "utf8").option("header", "true").csv(fsPath+"roadmatch_MathDriver/"+mon+"/0")

      val rapidDetail: DataFrame = calRapidSpeed(spark,dataSet)
      rapidDetail.repartition(1).write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath+"DZRapidScore/rapid/Details/"+mon)

      val rapidDetailAndEvent = eventFrequency(spark,rapidDetail)
      rapidDetailAndEvent.repartition(1).write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath+"DZRapidScore/rapid/statistics/"+mon)

      //基于驾驶员+载客状态统计
      val dfEHDetails = rapidDetail.groupBy("jsy","EHstatus","rapidMark").count().groupBy("jsy","EHstatus").pivot("rapidMark").sum("count").na.fill(0)
      val pivotEHDetails = dfEHDetails.withColumnRenamed("1","upEHSuddenCount")
        .withColumnRenamed("2","downEHSuddenCount")
        .withColumnRenamed("3","turnEHSuddenCount")
      pivotEHDetails.repartition(1).write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath+"DZRapidScore/rapid/EHstatus/"+mon)


    }
  }

  def calRapidSpeed(spark: SparkSession,dataSet: DataFrame):DataFrame = {
    dataSet.createOrReplaceTempView("ori_roadmatch")

    /**
      * 1过滤时间差在5s-60s区间内数据
      * 2过滤速度120km/h区间内数据
      * 3速度转化为m/s
      */

    //临时表：相邻时刻数据拉平(jsy,dbuscard,路段,路段类型,载客状态,经度1,纬度1,时间1,速度1,方向1,经度2,纬度2,时间2,速度2,方向2,时间间隔)
    //(jsy,dbuscard,wayname,waytype,EHstatus,prelon,prelat,prelutc,pretime,prespeed,preorien,llon,llat,lutc,longtime,speed,timeInterval,orientation)
    val sql1 =
      s"""
         |select
         |jsy,dbuscard,wayname,waytype,EHstatus,
         |prelon,prelat,prelutc,pretime,
         |prespeed/3.6 as prespeed,
         |preorien,
         |llon,llat,lutc,longtime,
         |speed/3.6 as speed,
         |orientation,
         |(longtime - pretime) as timeInterval
         |from(
         |select
         |jsy,dbuscard,wayname,waytype,EHstatus,
         |lag(llon,1,0) over(partition by jsy,dbuscard order by longtime) as prelon,
         |lag(llat,1,0) over(partition by jsy,dbuscard order by longtime) as prelat,
         |lag(lutc,1,0) over(partition by jsy,dbuscard order by longtime) as prelutc,
         |lag(longtime,1,0) over(partition by jsy,dbuscard order by longtime) as pretime,
         |lag(speed,1,0) over(partition by jsy,dbuscard order by longtime) as prespeed,
         |lag(carHeadAngle,1,0) over(partition by jsy,dbuscard order by longtime) as preorien,
         |llon,
         |llat,
         |lutc,
         |longtime,
         |speed,
         |carHeadAngle as orientation
         |from ori_roadmatch
         |having prelat <> 0 and speed<=120 and prespeed <=120 and (longtime - pretime)>=5 and (longtime - pretime)<=60) t
       """.stripMargin

    val tab_tmp1 = spark.sql(sql1)
    tab_tmp1.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tab_tmp1")

    val sql2 =
      s"""
         |select t.*,orient/timeInterval as angleSpeed
         |from (
         |select
         |*,
         |(speed - prespeed)/timeInterval as a_speed,
         |(speed + prespeed)/2  as avgSpeed,
         |case when
         |abs(orientation-preorien)>4 then 8-(abs(orientation-preorien))
         |else abs(orientation-preorien) end as orient,
         |Dist(prelon,prelat,llon,llat) as actualDist,
         |(speed + prespeed)/2 * timeInterval as avgDist,
         |theroyDistUpMin(prespeed,speed,timeInterval) as theroyDistUpMin,
         |theroyDistUpMax(prespeed,speed,timeInterval) as theroyDistUpMax,
         |theroyDistDnMin(prespeed,speed,timeInterval) as theroyDistDnMin,
         |theroyDistDnMax(prespeed,speed,timeInterval) as theroyDistDnMax
         |from tab_tmp1 ) t
       """.stripMargin
    spark.sql(sql2).persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tab_tmp2")

    val sql3 =
      s"""
         |select
         |avg_aspeed + 3 * std_aspeed as threshold_up,
         |avg_aspeed - 3 * std_aspeed as threshold_dn,
         |approx75_gap + 1.5*(approx75_gap - approx25_gap) as threshold_gap_min,
         |approx75_gap + 3*(approx75_gap - approx25_gap) as threshold_gap_max,
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
    spark.sql(sql3).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val threshold_up = spark.sql(sql3).first()(0).toString.toDouble
    val threshold_dn = -threshold_up
    val threshold_gap_min = spark.sql(sql3).first()(2).toString.toDouble
    val threshold_gap_max = spark.sql(sql3).first()(3).toString.toDouble
    println(threshold_up,threshold_gap_min,threshold_gap_max)

    val sql4 =
      s"""
         |select *,
         |case
         |when angleSpeed > $angleSpeedLimit and avgSpeed*3.6 > $speedLimit and orient>$angleLimit then 3
         |when a_speed > $threshold_up and (actualDist > theroyDistUpMax or actualDist < theroyDistUpMin)  and abs(actualDist - avgDist) > $threshold_gap_min and abs(actualDist - avgDist) < $threshold_gap_max
         |then 1
         |when a_speed < $threshold_dn and (actualDist > theroyDistDnMax or actualDist < theroyDistDnMin)  and abs(actualDist - avgDist) > $threshold_gap_min and abs(actualDist - avgDist) < $threshold_gap_max
         |then 2
         |else 0
         |end as rapidMark
         |from tab_tmp2
         |having rapidMark <> 0
       """.stripMargin
    spark.sql(sql4)

  }

}
