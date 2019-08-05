package com.sssjd.offline.taxi.stat

import com.sssjd.configure.LoadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object TurnCalc {

  def main(args: Array[String]): Unit = {

    val fsPath = LoadConfig.getHdfsConfiguration()
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.sql.shuffle.partitions", "100")
      .config("spark.default.parallelism", "500")
      .config("spark.local.dir","F:\\data")
      .appName("turnStat")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataSet = spark.read.option("encoding", "utf8").option("header", "true").csv(fsPath+"roadmatch_all/0/").persist(StorageLevel.MEMORY_AND_DISK_SER)

    dataSet.createOrReplaceTempView("ori_roadmatch")


    //临时表：相邻时刻数据拉平，并持久化（最后关联）
    //过滤速度大于120km/h,时间差在18-22s的数据
    /**
      *----------------------------------------------------------
      * jsy,pretime,prespeed,preorien,longtime,lspeed,orientation
      * ---------------------------------------------------------
      */
    val sql1 =
      s"""
         |select
         |jsy,
         |lag(longtime,1,0) over(partition by jsy order by longtime) as pretime,
         |lag(speed,1,0) over(partition by jsy order by longtime) as prespeed,
         |lag(carHeadAngle,1,0) over(partition by jsy order by longtime) as preorien,
         |longtime,
         |speed as lspeed,
         |carHeadAngle as orientation
         |from ori_roadmatch
         |where speed <=120
         |having (longtime - pretime)>=18 and (longtime - pretime)<=22
       """.stripMargin
    val tab_tmp1 = spark.sql(sql1)
    tab_tmp1.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("data1")


    // jsy,pretime,prespeed,preorien,longtime,lspeed,orientation,orient
    //过滤角度差大于等于45度(即大于等于1)
    val sql2 =
      s"""
         |select *,
         |CASE WHEN abs(orientation-preorien)> 4 THEN 8-(abs(orientation-preorien))
         |else abs(orientation-preorien) end AS orient
         |from data1
         |having orient>=1
       """.stripMargin

    val tab_tmp2 = spark.sql(sql2)
    tab_tmp2.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("data2")

    //计算角度差平均值
    val sql3 =
      s"""
         |select avg(orient) as avg_orient
         |from data2
         |where orient>=1
       """.stripMargin
    val avgOrient = spark.sql(sql3).first()(0)

    //过滤角度差大于等于平均值
    val sql4 =
      s"""
         |select *,$avgOrient as avg_orient
         |from data2
         |having orient >= avg_orient
       """.stripMargin

    spark.sql(sql4).persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("data3")

    //计算平均速度/角速度
    val sql5 =
      s"""
         |select *,
         |(prespeed+lspeed)/2 as avgSpeed,
         |orient/(longtime-pretime) as angleSpeed
         | from data3
       """.stripMargin
    spark.sql(sql5).persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("data4")


    val sql6 =
      s"""
         |select *,
         | avg_speed + 2 * std_speed as speedLimit,
         | avg_angle + 2 * std_angle as angleLimit
         | from(
         |select
         |AVG(avgSpeed) as avg_speed,
         |stddev(avgSpeed) as std_speed,
         |AVG(angleSpeed) as avg_angle,
         |stddev(angleSpeed) as std_angle
         |from data4) t
       """.stripMargin
    spark.sql(sql6).show()
    val stat = spark.sql(sql6).first()
    val avg_speed = stat(0)
    val std_speed = stat(1)
    val avg_angle = stat(2)
    val std_angle = stat(3)
    val speedLimit = stat(4)
    val angleLimit = stat(5)
    println(avg_speed,std_speed,avg_angle,std_angle,speedLimit,angleLimit,avgOrient)

//    spark.sql(sql6).createOrReplaceTempView("data5")
//
//    val sql7 =
//      s"""
//         |select *,$avg_speed,$std_speed,$avg_angle,$std_angle,$speedLimit,$angleLimit from ori_roadmatch
//       """.stripMargin
//    spark.sql(sql7).show()



  }

}
