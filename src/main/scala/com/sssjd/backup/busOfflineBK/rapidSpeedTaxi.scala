package com.sssjd.backup.busOfflineBK

import com.sssjd.configure.LoadConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object rapidSpeedTaxi {
  val n_std_up = 3
  val n_std_dn = 3
  val n_std_speed = 1.5
  val n_std_angle = 1.5
  System.setProperty("HADOOP_USER_NAME", LoadConfig.getHdfsUser())
  def main(args: Array[String]): Unit = {
    val fsPath = LoadConfig.getHdfsConfiguration()
    val spark: SparkSession = SparkSession.builder()
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "100")
      .config("spark.default.parallelism", "500")
      .appName("rapidOffline")
//      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    for(mon <- 0 until 1 ){
      val dataSet: DataFrame = spark.read.option("encoding", "utf8").option("header", "true").csv(fsPath+"roadmatch_all").persist(StorageLevel.MEMORY_AND_DISK_SER)
      val result = calRapidSpeed(spark,dataSet)
//      result.write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath+"rapid")
    }
  }

  def calRapidSpeed(spark: SparkSession,dataSet: DataFrame): DataFrame = {
    dataSet.createOrReplaceTempView("tmp_table")

    /**
      * 1过滤时间差在5s及20s区间内数据
      * 2过滤速度80km/h区间内数据
      * 3速度转化为m/s
      */
    val sql0 =
      s"""
         |select
         |jsy,llat,llon,longtime,
         |speed /3.6 as speed,
         |carHeadAngle,
         |wayname,
         |waytype,
         |lag(longtime,1,0) over(partition by jsy order by longtime) as pre
         |from tmp_table
         |where speed<=80
         |having (longtime - pre)>=5 and (longtime - pre)<= 20
       """.stripMargin
    spark.sql(sql0).createOrReplaceTempView("tab_tmp0")


    /**
      * 临时表：相邻时刻数据拉平，并持久化（最后关联）
      */
    val sql1 =
      s"""
         |select
         |jsy,
         |llat,
         |llon,
         |wayname,
         |waytype,
         |lag(longtime,1,0) over(partition by jsy order by longtime) as pretime,
         |lag(speed,1,0) over(partition by jsy order by longtime) as prespeed,
         |lag(carHeadAngle,1,0) over(partition by jsy order by longtime) as preorien,
         |longtime,
         |speed as lspeed,
         |carHeadAngle as orientation,
         |lead(longtime,1,0) over(partition by jsy order by longtime) as lattime,
         |lead(speed,1,0) over(partition by jsy order by longtime) as latspeed,
         |lead(carHeadAngle,1,0) over(partition by jsy order by longtime) as latorien
         |from tab_tmp0
       """.stripMargin
    val tab_tmp1 = spark.sql(sql1)
    tab_tmp1.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tab_tmp1")

    /**
      * 计算加速度
      */
    val sql2 =
      s"""
         |select
         |jsy,
         |longtime,
         |(pre_a+lat_a+tot_a)/3 as a_speed
         |from
         |(select
         |jsy,longtime,
         |(lspeed - prespeed)/(longtime-pretime) as pre_a,
         |(latspeed - lspeed)/(lattime-longtime) as lat_a,
         |(latspeed - prespeed)/(lattime-pretime) as tot_a
         |from tab_tmp1) as tmp
       """.stripMargin
    spark.sql(sql2).createOrReplaceTempView("tab_tmp2")

    /**
      * 计算加速度均值及标准差
      */
    val sql3 =
      s"""
         |select
         |jsy,
         |AVG(a_speed) as avg_speed,
         |stddev(a_speed) as std_speed
         |from tab_tmp2
         |group by jsy
       """.stripMargin
    spark.sql(sql3).createOrReplaceTempView("tab_tmp3")


    /**
      * 计算加速度上下限
      */
    val sql4 =
      s"""
         |select
         |tab_tmp1.*,tab_tmp2.a_speed, tab_tmp3.avg_speed,tab_tmp3.std_speed,
         |(tab_tmp3.avg_speed + ${n_std_up}*tab_tmp3.std_speed) as up_limit,
         |(tab_tmp3.avg_speed - ${n_std_dn}*tab_tmp3.std_speed) as dn_limit
         |from
         |tab_tmp1
         |left join tab_tmp2 on tab_tmp1.jsy =tab_tmp2.jsy and tab_tmp1.longtime = tab_tmp2.longtime
         |left join tab_tmp3 on tab_tmp1.jsy =tab_tmp3.jsy
       """.stripMargin
    spark.sql(sql4).createOrReplaceTempView("tab_tmp4")


    val sq =
      s"""
         |select distinct jsy,avg_speed,std_speed,up_limit,dn_limit
         |from tab_tmp4

       """.stripMargin

    spark.sql(sq).repartition(1).write.mode(SaveMode.Append).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv("hdfs://192.168.100.177:8020/user/root/VDP_taxi/2017analyse/rapidStat/")

    /**
      * 计算急加急减速
      */
    val sql5 =
      s"""
         |select *,
         |case when a_speed > up_limit then 4
         |when a_speed < dn_limit then 2
         |/* when a_speed < (-1 * up_limit) then 2 */
         |else 0
         |end as rapidMark
         |from tab_tmp4
         |having rapidMark <> 0
       """.stripMargin
    spark.sql(sql5).createOrReplaceTempView("tab_tmp5")


    /**
      * 去除相邻状态相同记录
      */
    val sql6 =
      s"""
         |select tab.*,
         |lag(tab.longtime,1,0) over(partition by tab.jsy order by tab.longtime) as pTime,
         |lag(tab.rapidMark,1,0) over(partition by tab.jsy order by tab.longtime) as pMark
         |from tab_tmp5 as tab
         |having (tab.longtime - pTime)>15 or tab.rapidMark <> pMark
       """.stripMargin
    spark.sql(sql6).createOrReplaceTempView("tab_tmp6")


    /**
      * 急加急减速结果
      */

    val sql7 =
      s"""
         |select
         |t.jsy,t.llat,t.llon,t.wayname,t.waytype,
         |t.pretime,t.prespeed,t.preorien,t.longtime,t.lspeed,t.orientation,
         |t.lattime,t.latspeed,t.latorien,t.rapidMark
         |from tab_tmp6 as t
         |where t.rapidMark <> 0
       """.stripMargin
    val tmp_rapid =  spark.sql(sql7)
    tmp_rapid.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tmp_rapid")



    /**
      *急转弯：计算平均速度及方向幅度
      */
    val sql8 =
      s"""
         |select
         |jsy,longtime,pretime,
         |(prespeed+lspeed)/2 as avgSpeed,
         |case when
         |abs(orientation-preorien)>4 then 8-(abs(orientation-preorien))
         |else abs(orientation-preorien) end as orient
         |from tab_tmp1
       """.stripMargin
    spark.sql(sql8).createOrReplaceTempView("tab_tmp8")

    /**
      * 计算方向角
      */

    val sql9 =
      s"""
         |select
         |jsy,longtime,avgSpeed,orient,
         |orient/(longtime-pretime) as angleSpeed
         |from tab_tmp8
         |-- where avgSpeed>5.55 and orient>45
       """.stripMargin
    spark.sql(sql9).createOrReplaceTempView("tab_tmp9")

    /**
      * 计算平均速度均值/标准差及方向角均值/标准差
      */

    val sql10 =
      s"""
         |select
         |jsy,
         |AVG(avgSpeed) as avg_speed,
         |stddev(avgSpeed) as std_speed,
         |AVG(angleSpeed) as avg_angle,
         |stddev(angleSpeed) as std_angle
         |from tab_tmp9
         |group by jsy
       """.stripMargin
    spark.sql(sql10).createOrReplaceTempView("tab_tmp10")

    /**
      * 计算急转弯
      */

    val sql11 =
      s"""
         |select
         |tab_tmp1.*,
         |tab_tmp9.avgSpeed,
         |tab_tmp9.angleSpeed,
         |tab_tmp10.avg_speed,
         |tab_tmp10.std_speed,
         |tab_tmp10.avg_angle,
         |tab_tmp10.std_angle,
         |(tab_tmp10.avg_speed + ${n_std_speed}*tab_tmp10.std_speed) as v_limit,
         |(tab_tmp10.avg_angle + ${n_std_angle}*tab_tmp10.std_angle) as g_limit,
         |1 as angleMark
         |from tab_tmp1
         |left join tab_tmp9
         |on tab_tmp1.jsy =tab_tmp9.jsy and tab_tmp1.longtime = tab_tmp9.longtime
         |left join tab_tmp10
         |on tab_tmp1.jsy =tab_tmp10.jsy
         |where tab_tmp9.avgSpeed is not null and tab_tmp9.angleSpeed is not null
         |having tab_tmp9.avgSpeed>v_limit and tab_tmp9.angleSpeed>g_limit
       """.stripMargin
    spark.sql(sql11).createOrReplaceTempView("tmp_angle")
    val tmp_angle = spark.sql(sql11)


    val sq000 =
      s"""
         |select distinct jsy,avg_angle,std_angle,g_limit,avg_speed,std_speed,v_limit
         | from tmp_angle

       """.stripMargin

    spark.sql(sq000).repartition(1).write.mode(SaveMode.Append).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv("hdfs://192.168.100.177:8020/user/root/VDP_taxi/2017analyse/rapidStatAngle/")




//    /**
//      * 匹配路口
//      */
//
//
//    val sql12 =
//      s"""
//         |select
//         |t.jsy,
//         |t.llat,
//         |t.llon,
//         |t.wayname,
//         |t.waytype,
//         |t.roadsection,
//         |flag,
//         |direcflag,
//         |t.pretime,
//         |t.prespeed,
//         |t.preorien,
//         |t.longtime,
//         |t.lspeed,
//         |t.orientation,
//         |case when t.precross is not null then t.precross
//         |when t.roadcross is not null then t.roadcross
//         |when t.latcross is not null then t.latcross
//         |else null end iscross,
//         |t.lattime,
//         |t.latspeed,
//         |t.latorien,
//         |t.angleMark
//         |from tab_tmp11 as t
//         |having iscross is not null
//       """.stripMargin
//    val tmp_angle = spark.sql(sql12)
//    tmp_angle.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tmp_angle")


    /**
      * 关联三急状态
      */

    val sql13 =
      s"""
         |select
         |ifnull(t1.jsy,t2.jsy) as jsy,
         |ifnull(t1.longtime,t2.longtime) as longtime,
         |t1.rapidMark,
         |t2.angleMark
         |from
         |(select jsy,longtime,rapidMark from tmp_rapid) as t1
         |full outer join (select jsy,longtime,angleMark from tmp_angle) as t2
         |on t1.jsy=t2.jsy and t1.longtime=t2.longtime
       """.stripMargin
    spark.sql(sql13).createOrReplaceTempView("tab_tmp13")

    /**
      * 匹配详细信息
      */

    val sql14 =
      s"""
         |select
         |tab_tmp1.jsy as userId,
         |tab_tmp1.llat as lat,
         |tab_tmp1.llon as lon,
         |tab_tmp1.pretime as preTimestamp,
         |tab_tmp1.longtime as timestamp,
         |tab_tmp1.lattime as latTimestamp,
         |tab_tmp1.prespeed * 3.6 as preSpeed,
         |tab_tmp1.lspeed * 3.6 as speed,
         |tab_tmp1.latspeed * 3.6 as latSpeed,
         |tab_tmp1.preorien as preOrientation,
         |tab_tmp1.orientation as orientation,
         |tab_tmp1.latorien as latOrientation,
         |tab_tmp1.wayname as wayName,
         |tab_tmp1.waytype as wayType,
         |tab_tmp13.rapidMark as rapidChange,
         |tab_tmp13.angleMark as rapidTurn
         |from tab_tmp1
         |join tab_tmp13
         |on tab_tmp1.jsy = tab_tmp13.jsy and tab_tmp1.longtime=tab_tmp13.longtime
       """.stripMargin
    val result = spark.sql(sql14)
    tab_tmp1.unpersist()
    tmp_rapid.unpersist()
    tmp_angle.unpersist()
    result
  }

}
