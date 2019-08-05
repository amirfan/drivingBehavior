package com.sssjd.offline.taxi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ts extends Serializable {

  def main(args: Array[String]): Unit = {



    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("header", "true").csv("F:\\xxx/")
    eventFrequency(spark,df)

  }

  def eventFrequency(spark:SparkSession, rapidDetail: Dataset[Row]):DataFrame ={

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val separator = "_"
    val df = rapidDetail.select(concat_ws(separator,col("jsy"),col("rapidMark")).as("jsyStat"),col("longtime"))

    val dfDetails = rapidDetail.groupBy("jsy","rapidMark").count().groupBy("jsy").pivot("rapidMark").sum("count").na.fill(0)

    val pivotDetails = dfDetails.withColumnRenamed("1","upSuddenCount")
      .withColumnRenamed("2","downSuddenCount")
      .withColumnRenamed("3","turnSuddenCount")

    pivotDetails.show()

    val dfEHDetails = rapidDetail.groupBy("jsy","EHstatus","rapidMark").count().groupBy("jsy","EHstatus").pivot("rapidMark").sum("count").na.fill(0)
    val pivotEHDetails = dfEHDetails.withColumnRenamed("1","upEHSuddenCount")
      .withColumnRenamed("2","downEHSuddenCount")
      .withColumnRenamed("3","turnEHSuddenCount")
    pivotEHDetails.show()



    val arr: RDD[(String, List[Long])] = df.rdd.map(row =>{
      (row(0).toString,row(1).toString.toLong)
    }).groupByKey().map(row =>{
      (row._1,row._2.toList)
    })

    val dfCnt = arr.map(row =>calEventNumbers(row._1,row._2)).map(row =>{
      val res = row._1.split("_")
      (res(0),res(1),row._2)
    }).toDF("jsy","status","cnt")

    val pivotDF = dfCnt.groupBy("jsy").pivot("status").sum("cnt").na.fill(0)

    val pivotEvent = pivotDF.withColumnRenamed("1","upSuddenEvent")
      .withColumnRenamed("2","downSuddenEvent")
      .withColumnRenamed("3","turnSuddenEvent")

    pivotDetails.join(pivotEvent,"jsy")

  }


  def calEventNumbers(key:String,value:List[Long]) ={
    var cnt = 0
    //起始时刻状态(时间,标志位)
    var startStatus = (value.head,0)
    val vIterator = value.tail.toIterator
    while(vIterator.hasNext){
      val start = startStatus._1
      val status = startStatus._2
      val end = vIterator.next()
      if((end - start)<=180){
        if(status==0){
          cnt = cnt + 1
          startStatus = (start,1)
        }
      }else{
        //时间间隔大于180s,重新赋予起始时间
        startStatus = (end,0)
      }
    }
    (key,cnt)
  }

}
