package com.sssjd.backup.eventBK

import redis.clients.jedis.Jedis

import scala.collection.mutable

trait EventFrequency extends Serializable {

  //判断三分钟内数据是否有连续
  //记录第一次急加速(急减/急转)事件起始时刻+状态(0)
  //在满足3分钟条件下,第二次急加速事件到达,保存,更改状态(1)
  //在满足3分钟条件下,第三次急加速事件到达,发送,更改状态(2)
  //在满足3分钟时间条件下,判断状态为2时,后续急加速事件到达,不发送消息事件
  //不满足3分钟时间条件下,记录第一次急加速(急减/急转)事件起始时刻+状态(0)
  //发送具体格式：起始时刻+3分钟和急加速值

  def eventStatus(st:String,jsy:String,preGPS:List[String],curGps:List[String],curList:List[String],jedis: Jedis) ={

    val key = jsy+":"+st
    val newstat = mutable.HashMap[String,String]()
    val status = jedis.hget("taxiPreStatus", key)

    if(status != null){
      val start_time = status.split("_")(0).toLong
      val pre_time = status.split("_")(1).toLong
      val end_time = start_time + 180000
      val mark = status.split("_")(2)
      val now = curList(4).toLong

      if (now > pre_time && now < end_time) {
        if(mark == "0"){
          val event = start_time + "_"+now+"_" + "1"
          jedis.hset("taxiPreStatus",key, event)
        }
        else if(mark== "1"){
          val event = start_time + "_" + now + "_" + "2"
          jedis.hset("taxiPreStatus",key,event)
          newstat +=("dbuscard"->curList(1))
          newstat +=("dguid"->jsy)
          newstat +=("starttime"->start_time.toString)
          newstat +=("endtime"->end_time.toString)
          newstat +=("alarmtype"->st)
        }
      }
      //超过3分钟时间条件下,记录第一次急加速(急减/急转)事件起始时刻+状态(0)
      else {
        val starttm = preGPS(5)
        val curentm = curGps.head
        val event = starttm+"_"+curentm+"_"+"0"
        jedis.hset("taxiPreStatus", key, event)
      }
    }
    //记录第一次急加速(急减/急转)事件起始时刻+状态(0)
    else{
      val starttm = preGPS(5)
      val curentm = curGps.head
      val event = starttm+"_"+curentm+"_"+"0"
      jedis.hset("taxiPreStatus", key, event)
    }
    newstat
  }

}
