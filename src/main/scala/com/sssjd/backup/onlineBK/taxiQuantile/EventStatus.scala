package com.sssjd.backup.onlineBK.taxiQuantile

import java.util

import redis.clients.jedis.Jedis

import scala.collection.mutable

trait EventStatus {

  //判断三分钟内数据是否有连续
  //记录第一次急加速(急减/急转)事件起始时刻+状态(0)
  //在满足3分钟条件下,第二次急加速事件到达,发送消息事件,更改状态(1)
  //在满足3分钟时间条件下,判断状态为1时,后续急加速事件到达,不发送消息事件
  //不满足3分钟时间条件下,记录第一次急加速(急减/急转)事件起始时刻+状态(0)
  //发送具体格式：起始时刻+3分钟和急加速值

  def eventStatus(st:String,jsy:String,preGPS:List[String],curGps:List[String],curList:List[String],jedis: Jedis): String ={

    val key = jsy+":"+st
    var newstat:String = null
    val status = jedis.hget("taxiPreStatus", key)

    if(status != null){

      val start_time = status.split("_")(0).toLong
      val pre_time = status.split("_")(1).toLong
      val end_time = pre_time + 180000
      val now = curList(4).toLong

      val mark = status.split("_")(2)

      if (now > pre_time && now < end_time) {

        if(mark == "0"){
//          newstat = curList ++ List(st)
          val event = start_time + "_"+now+"_" + "1"
          jedis.hset("taxiPreStatus",key, event)
          newstat = start_time.toString+"_"+now+"_"+end_time.toString+"_"+jsy+"_"+st
        }
        else{
          newstat = null
        }
      }
      //超过3分钟时间条件下,记录第一次急加速(急减/急转)事件起始时刻+状态(0)
      else {
        newstat = null
        val event = preGPS(5)+"_"+curGps.head+"_"+"0"
        jedis.hset("taxiPreStatus", key, event)
      }
    }
    //记录第一次急加速(急减/急转)事件起始时刻+状态(0)
    else{
      newstat = null
      val event = preGPS(5)+"_"+curGps.head+"_"+"0"
      jedis.hset("taxiPreStatus", key, event)
    }
    newstat
  }

}
