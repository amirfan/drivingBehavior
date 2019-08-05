package com.sssjd.utils

import scala.math._

object UDFDistance extends Serializable {

  val getDist = (lut1: Double, lat1: Double, lut2: Double, lat2: Double) => {
    val rad: Double = 6378.137
    val lts = sin(pow(abs(lat1 - lat2) * Pi / 360, 2))
    val lns = sin(pow(abs(lut1 - lut2) * Pi / 360, 2))
    val ltc = cos(lat1 * Pi / 180) * cos(lat2 * Pi / 180)
    val distance = 2 * asin(sqrt(lts + lns * ltc)) * rad
    distance * 1000
  }


  val theroyDistUpMin = (startSpeed: Double, endSpeed: Double, timeInterval: Double) => {
    val a = 2.78
    val tmin = 2
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v1 - v0) / a
    val s = t match {
      case _ if t >= tmin => {
        val s1 = v0 * (timeInterval - t)
        val s2 = (v0 + v1) / 2 * t
        s1 + s2
      }
      case _ if t > 0 && t < tmin => {
        val v_mid = v1 - a * tmin
        val v = v_mid match {
          case _ if v_mid > 0 => v_mid
          case _ => 0
        }
        val s1 = (v0 + v) / 2 * (timeInterval - tmin)
        val s2 = (v + v1) / 2 * tmin
        s1 + s2
      }
      case _ => 0
    }
    s
  }

  val theroyDistUpMax = (startSpeed: Double, endSpeed: Double, timeInterval: Double) => {
    val a = 2.78
    val tmin = 2
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v1 - v0) / a
    val s = t match {
      case _ if t >= tmin => {
        val s1 = (v0 + v1) / 2 * t
        val s2 = v1 * (timeInterval - t)
        s1 + s2
      }
      case _ if t > 0 && t < tmin => {
        val v = v0 + a * tmin
        val s1 = (v0 + v) / 2 * tmin
        val s2 = (v + v1) / 2 * (timeInterval - tmin)
        s1 + s2
      }
      case _ => 0
    }
    s
  }


  val theroyDistDnMin = (startSpeed: Double, endSpeed: Double, timeInterval: Double) => {
    val a = 2.78
    val tmin = 2
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v0 - v1) / a
    val s = t match {
      case _ if t >= tmin => {
        val s1 = (v0 + v1) / 2 * t
        val s2 = v1 * (timeInterval - t)
        s1 + s2
      }
      case _ if t > 0 && t < tmin => {
        val v_mid = v0 - a * tmin
        val v = v_mid match {
          case _ if v_mid > 0 => v_mid
          case _ => 0
        }
        val s1 = (v0 + v) / 2 * tmin
        val s2 = (v + v1) / 2 * (timeInterval - tmin)
        s1 + s2
      }
      case _ => 0
    }
    s
  }


  val theroyDistDnMax = (startSpeed: Double, endSpeed: Double, timeInterval: Double) => {
    val a = 2.78
    val tmin = 2
    val v0 = startSpeed / 3.6
    val v1 = endSpeed / 3.6
    val t = (v0 - v1) / a
    val s = t match {
      case _ if t >= tmin => {
        val s1 = v0 * (timeInterval - t)
        val s2 = (v0 + v1) / 2 * t
        s1 + s2
      }
      case _ if t > 0 && t < tmin => {
        val v = v1 + a * tmin
        val s1 = (v0 + v) / 2 * (timeInterval - tmin)
        val s2 = (v + v1) / 2 * tmin
        s1 + s2
      }
      case _ => 0
    }
    s
  }
}
