package com.sssjd.utils

import java.text.SimpleDateFormat
import java.util.Calendar

object TimeUtil {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()

  def apply(time:String) ={
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  def getCertainDayTime(amount:Int):Long={
    calendar.add(Calendar.DATE,amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE,-amount)
    time
  }

  def main(args: Array[String]): Unit = {
    val s =TimeUtil("2019-07-15 16:07:41")
    println(s)
  }

}
