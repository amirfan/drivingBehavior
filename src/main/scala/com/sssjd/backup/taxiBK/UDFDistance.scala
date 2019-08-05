package com.sssjd.backup.taxiBK

import scala.math._

object UDFDistance {

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
    val v0 = startSpeed
    val v1 = endSpeed
    val t2 = 2
    val t1 = timeInterval - t2
    val v_mid = v1 - a * t2
    val v = v_mid match {
      case _ if v_mid>0 => v_mid
      case _ => 0
    }
    val s1 = (v0 + v)/2 * t1
    val s2 = v * t2 + 1/2 * a * pow(t2,2)
    s1 + s2
  }


  val theroyDistUpMax = (startSpeed:Double,endSpeed:Double,timeInterval:Double)=>{

    val a = 2.78
    val v0 = startSpeed
    val v1 = endSpeed
    val t1 = 2
    val t2 = timeInterval - t1
    val v = v0 + a * t1
    val s1 = v0 * t1 + 1/2 * a * pow(t1,2)
    val s2 = (v + v1)/2 * t2
    s1 + s2
  }


  val theroyDistDnMin = (startSpeed:Double,endSpeed:Double,timeInterval:Double)=>{

    val a = 2.78
    val v0 = startSpeed
    val v1 = endSpeed
    val t1 = 2
    val t2 = timeInterval - t1
    val v_mid = v0 - a * t1

    val v = v_mid match {
      case _ if v_mid>0 => v_mid
      case _ => 0
    }
    val s1 = v0 * t1 - 1/2 * a * pow(t1,2)
    val s2 = (v + v1)/2 * t2
    s1 + s2
  }


  val theroyDistDnMax = (startSpeed:Double,endSpeed:Double,timeInterval:Double) =>{

    val a = 2.78
    val v0 = startSpeed
    val v1 = endSpeed
    val t2 = 2
    val t1 = timeInterval - t2
    val v = v1 + a * t2
    val s1 = (v0 + v)/2 * t1
    val s2 = v * t2 - 1/2 * a * pow(t2,2)
    s1 + s2
  }


}
