package com.sssjd.offline.taxi.evaluate

import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import scala.collection.mutable.ArrayBuffer


/**
  * 计算特征项
  * 根据HDFS原始特征统计数据
  */

object Features{

  case class DriverBehavior(uid:String,mileage:Double,overCnt:Double,overLevel:Double,overTime:Double,rapidUpCount:Double,rapidDownCount:Double,rapidTurnCount:Double)
  case class DriverBehaviorEvent(uid:String,overEventCnt:Double,overEventLevel:Double,overEventTime:Double,rapidUpEventCount:Double,rapidDownEventCount:Double,rapidTurnEventCount:Double)

  //明细
  def calFeature(feature: DataFrame,weight: (List[Double], List[ArrayBuffer[Double]])):Dataset[DriverBehavior]={
    val scoreDetails: Dataset[DriverBehavior] = feature.map(row=>{
      val uid = row.getAs[String]("jsy")  //jsy

      val mileageACount  = row.getAs[String]("liftkilometers").toDouble/row.getAs[String](1).toDouble  //重车里程
      val mileageBCount  = row.getAs[String]("emptykilometers").toDouble/row.getAs[String](1).toDouble  //空车里程
      val mileage = weight._2(1)(0) * mileageACount +  weight._2(1)(1) * mileageBCount                          //里程(载客越多,里程越少,分值越高)

      val overCnt = row.getAs[String]("overspeedcount").toDouble match {                        //超速次数
        case 0.0 => 1.0/ row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("overspeedcount").toDouble+1) / row.getAs[String](1).toDouble
      }

      val sectionACount  = row.getAs[String]("overspeedA").toDouble match {   //超速等级A
        case 0.0 => 1.0/ row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("overspeedA").toDouble+1) / row.getAs[String](1).toDouble
      }
      val sectionBCount  = row.getAs[String]("overspeedB").toDouble match {   //超速等级B
        case 0.0 => 1.0/ row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("overspeedB").toDouble+1) / row.getAs[String](1).toDouble
      }
      val sectionCCount  = row.getAs[String]("overspeedC").toDouble match {   //超速等级C
        case 0.0 => 1.0/ row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("overspeedC").toDouble+1) / row.getAs[String](1).toDouble
      }

      val overLevel = weight._2(0)(0) * sectionACount + weight._2(0)(1) * sectionBCount + weight._2(0)(2) * sectionCCount  //超速等级

      val overTime = row.getAs[String]("overspeedTime").toDouble match {  //超速时长
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("overspeedTime").toDouble+1) / row.getAs[String](1).toDouble
      }

      val rapidUpCount = row.getAs[String]("upSuddenCount").toDouble match {  //急加速次数
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("upSuddenCount").toDouble+1) / row.getAs[String](1).toDouble
      }
      val rapidDownCount = row.getAs[String]("downSuddenCount").toDouble match {  //急减速次数
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("downSuddenCount").toDouble+1) / row.getAs[String](1).toDouble
      }
      val rapidTurnCount = row.getAs[String]("turnSuddenCount").toDouble match {  //急转弯次数
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("turnSuddenCount").toDouble+1) / row.getAs[String](1).toDouble
      }


      DriverBehavior(uid,mileage,overCnt,overLevel,overTime,rapidUpCount,rapidDownCount,rapidTurnCount)
    })(Encoders.product[DriverBehavior])
    scoreDetails
  }


  //事件
  def calEventFeature(feature: DataFrame,weight: (List[Double], List[ArrayBuffer[Double]])):Dataset[DriverBehaviorEvent]={
    val scoreEvents: Dataset[DriverBehaviorEvent] = feature.map(row=> {
      val uid = row.getAs[String]("jsy")
      val overEventCount = row.getAs[String]("overspeedEventcount").toDouble / row.getAs[String](1).toDouble //超速事件次数
      val overspeedEventB = row.getAs[String]("overspeedEventB").toDouble / row.getAs[String](1).toDouble //超速事件等级B
      val overspeedEventC = row.getAs[String]("overspeedEventC").toDouble / row.getAs[String](1).toDouble //超速事件等级C
      val overEventLevel = weight._2(0)(0) * overspeedEventB + weight._2(0)(1) * overspeedEventC  //超速事件等级
      val overEventTime = row.getAs[String]("overspeedEventTime").toDouble / row.getAs[String](1).toDouble //超速事件时长
      val rapidUpEventCount = row.getAs[String]("upSuddenEvent").toDouble / row.getAs[String](1).toDouble //急加速事件次数
      val rapidDownEventCount = row.getAs[String]("downSuddenEvent").toDouble / row.getAs[String](1).toDouble //急减速事件次数
      val rapidTurnEventCount = row.getAs[String]("turnSuddenEvent").toDouble / row.getAs[String](1).toDouble //急转弯事件次数

      DriverBehaviorEvent(uid,overEventCount,overEventLevel,overEventTime,rapidUpEventCount,rapidDownEventCount,rapidTurnEventCount)
  })(Encoders.product[DriverBehaviorEvent])

  scoreEvents
  }

}
