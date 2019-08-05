package com.sssjd.backup.eventBK

import scala.math._

object UDFDistance extends Serializable {

  val getDist = (lut1:Double,lat1:Double,lut2:Double,lat2:Double) =>{
    val rad:Double = 6378.137
    val lts = sin(pow(abs(lat1 - lat2)*Pi/360,2))
    val lns = sin(pow(abs(lut1 - lut2)*Pi/360,2))
    val ltc = cos(lat1 * Pi /180) * cos(lat2 * Pi / 180)
    val distance = 2 * asin(sqrt(lts + lns * ltc)) * rad
    distance * 1000
  }


  val theroyDistUpMin = (startSpeed:Double,endSpeed:Double,timeInterval:Double)=>{
    val a = 2.78
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v1-v0)/a
    val t2 = t match {   //加速时间
      case _ if t >= 2  => t
      case _ if t>0 && t<2 => 2
      case _ => 0
    }
    val t1 = timeInterval - t2 //匀速时间
    val s1 = v0 * t1           //匀速里程
    val s2 = (v0 + v1)/2 * t2  //加速里程
    s1 + s2
  }

  val theroyDistUpMax = (startSpeed:Double,endSpeed:Double,timeInterval:Double)=>{
    val a = 2.78
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v1-v0)/a
    val t1 = t match {   //加速时间
      case _ if t >= 2  => t
      case _ if t>0 && t<2 => 2
      case _ => 0
    }
    val t2 = timeInterval - t1 //匀速时间
    val s1 = (v0+v1)/2*t1  //加速里程
    val s2 = v1 * t2       //匀速里程
    s1 + s2
  }



  val theroyDistDnMin = (startSpeed:Double,endSpeed:Double,timeInterval:Double)=>{
    val a = 2.78
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v0-v1)/a
    val t1 = t match {
      case _ if t >= 2  => t
      case _ if t>0 && t<2 => 2
      case _ => 0
    }
    val t2 = timeInterval - t1
    val s1 = (v0+v1)/2*t1
    val s2 = v1 * t2
    s1 + s2
  }


  val theroyDistDnMax = (startSpeed:Double,endSpeed:Double,timeInterval:Double) =>{
    val a = 2.78
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v0-v1)/a
    val t2 = t match {
      case _ if t >= 2  => t
      case _ if t>0 && t<2 => 2
      case _ => 0
    }
    val t1 = timeInterval - t2
    val s1 = v0 * t1
    val s2 = (v0+v1)/2*t2
    s1 + s2
  }


}
