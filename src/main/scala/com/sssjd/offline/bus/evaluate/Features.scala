package com.sssjd.offline.bus.evaluate

import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import scala.collection.mutable.ArrayBuffer

/**
  * 计算特征项
  * 根据HDFS原始特征统计数据
  */

object Features{

  case class DriverBehavior(uid:String,overCnt:Double,overLevel:Double,sectionTime:Double,rapidUpCount:Double,rapidDownCount:Double,rapidTurnCount:Double)
  case class DriverBehaviorEvent(uid:String,overEventCnt:Double,overEventLevel:Double,sectionEventTime:Double,rapidUpEventCount:Double,rapidDownEventCount:Double,rapidTurnEventCount:Double)

  //明细
  def calFeature(feature: DataFrame,weight: (List[Double], List[ArrayBuffer[Double]])):Dataset[DriverBehavior]={
    val scoreDetails: Dataset[DriverBehavior] = feature.map(row=>{
      val uid = row.getAs[String]("userId")  //jsy

      val sectionCount = row.getAs[String]("sectionCount").toDouble match {                        //路段超速次数
        case 0.0 => 1.0/ row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("sectionCount").toDouble+1) / row.getAs[String](1).toDouble
      }
      val crossCount = row.getAs[String]("crossCount").toDouble/row.getAs[String](1).toDouble      //路口超速次数
      val stationCount = row.getAs[String]("stationCount").toDouble/row.getAs[String](1).toDouble  //站点超速次数
      val overCnt = weight._2(0)(0) * sectionCount + weight._2(0)(1)*crossCount + weight._2(0)(2)*stationCount  //超速次数

      val sectionACount  = row.getAs[String]("sectionACount").toDouble/row.getAs[String](1).toDouble  //路段超速等级A
      val sectionBCount  = row.getAs[String]("sectionBCount").toDouble/row.getAs[String](1).toDouble  //路段超速等级B
      val sectionCCount  = row.getAs[String]("sectionCCount").toDouble/row.getAs[String](1).toDouble  //路段超速等级C
      val sectionLevel = weight._2(2)(0) * sectionACount + weight._2(2)(1) * sectionBCount + weight._2(2)(2) * sectionCCount  //路段超速等级

      val stationACount  = row.getAs[String]("stationACount").toDouble/row.getAs[String](1).toDouble  //站点超速等级A
      val stationBCount  = row.getAs[String]("stationBCount").toDouble/row.getAs[String](1).toDouble  //站点超速等级B
      val stationCCount  = row.getAs[String]("stationCCount").toDouble/row.getAs[String](1).toDouble  //站点超速等级C
      val stationLevel = weight._2(3)(0) * stationACount + weight._2(3)(1) * stationBCount + weight._2(3)(2) * stationCCount //站点超速等级

      val crossACount  = row.getAs[String]("crossACount").toDouble/row.getAs[String](1).toDouble    //路口超速等级A
      val crossBCount  = row.getAs[String]("crossBCount").toDouble/row.getAs[String](1).toDouble    //路口超速等级B
      val crossCCount  = row.getAs[String]("crossCCount").toDouble/row.getAs[String](1).toDouble   //路口超速等级C
      val crossLevel = weight._2(4)(0) * crossACount + weight._2(4)(1) * crossBCount + weight._2(4)(2) * crossCCount  //路口超速等级

      val overLevel =  weight._2(1)(0) * sectionLevel + weight._2(1)(1) * crossLevel + weight._2(1)(2) * stationLevel  //超速等级

      val sectionTime = row.getAs[String]("sectionTime").toDouble match {  //超速时长
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("sectionTime").toDouble+1) / row.getAs[String](1).toDouble
      }

      val rapidUpCount = row.getAs[String]("upperSudden").toDouble match {  //急加速次数
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("upperSudden").toDouble+1) / row.getAs[String](1).toDouble
      }
      val rapidDownCount = row.getAs[String]("lowSudden").toDouble match {  //急减速次数
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("lowSudden").toDouble+1) / row.getAs[String](1).toDouble
      }
      val rapidTurnCount = row.getAs[String]("turnSudden").toDouble match {  //急转弯次数
        case 0.0 => 1.0 /row.getAs[String](1).toDouble
        case _ => (row.getAs[String]("turnSudden").toDouble+1) / row.getAs[String](1).toDouble
      }


      DriverBehavior(uid,overCnt,overLevel,sectionTime,rapidUpCount,rapidDownCount,rapidTurnCount)
    })(Encoders.product[DriverBehavior])
    scoreDetails
  }


  //事件
  def calEventFeature(feature: DataFrame,weight: (List[Double], List[ArrayBuffer[Double]])):Dataset[DriverBehaviorEvent]={
    val scoreEvents: Dataset[DriverBehaviorEvent] = feature.map(row=> {
      val uid = row.getAs[String]("userId")

      val sectionCount = row.getAs[String]("sectionCountEvent").toDouble / row.getAs[String](1).toDouble //路段超速次数
      val crossCount = row.getAs[String]("crossCountEvent").toDouble / row.getAs[String](1).toDouble //路口超速次数
      val stationCount = row.getAs[String]("stationCountEvent").toDouble / row.getAs[String](1).toDouble //站点超速次数
      val overEventCnt = weight._2(0)(0) * sectionCount + weight._2(0)(1) * crossCount + weight._2(0)(2) * stationCount //超速次数

      val sectionBCount = row.getAs[String]("sectionBCountEvent").toDouble / row.getAs[String](1).toDouble //路段超速等级B
      val sectionCCount = row.getAs[String]("sectionCCountEvent").toDouble / row.getAs[String](1).toDouble //路段超速等级C
      val sectionLevel = weight._2(2)(0) * sectionBCount + weight._2(2)(1) * sectionCCount //路段超速等级

      val stationBCount = row.getAs[String]("stationBCountEvent").toDouble / row.getAs[String](1).toDouble //站点超速等级B
      val stationCCount = row.getAs[String]("stationCCountEvent").toDouble / row.getAs[String](1).toDouble //站点超速等级C
      val stationLevel = weight._2(3)(0) * stationBCount + weight._2(3)(1) * stationCCount //站点超速等级

      val crossBCount = row.getAs[String]("crossBCountEvent").toDouble / row.getAs[String](1).toDouble //路口超速等级B
      val crossCCount = row.getAs[String]("crossCCountEvent").toDouble / row.getAs[String](1).toDouble //路口超速等级C
      val crossLevel = weight._2(4)(0) * crossBCount + weight._2(4)(1) * crossCCount //路口超速等级

      val overEventLevel = weight._2(1)(0) * sectionLevel + weight._2(1)(1) * stationLevel + weight._2(1)(2) * crossLevel //超速等级

      val sectionEventTime = row.getAs[String]("sectionTimeEvent").toDouble / row.getAs[String](1).toDouble //超速时长
      val rapidUpEventCount = row.getAs[String]("upperSudden2").toDouble / row.getAs[String](1).toDouble //急加速次数
      val rapidDownEventCount = row.getAs[String]("lowSudden2").toDouble / row.getAs[String](1).toDouble //急减速次数
      val rapidTurnEventCount = row.getAs[String]("turnSudden2").toDouble / row.getAs[String](1).toDouble //急转弯次数

      DriverBehaviorEvent(uid,overEventCnt,overEventLevel,sectionEventTime,rapidUpEventCount,rapidDownEventCount,rapidTurnEventCount)
  })(Encoders.product[DriverBehaviorEvent])

  scoreEvents
  }

}
