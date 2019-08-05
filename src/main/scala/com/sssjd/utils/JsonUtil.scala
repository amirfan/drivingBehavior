package com.sssjd.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}


object JsonUtil {


  val mapper: ObjectMapper = new ObjectMapper()

  /**
    * Object 转 jsonString 方法
    * (hc add 20170628 带测试fastjson)
    * @param T
    * @return     jsonString
    */
  def toJsonString(T: Object): String = {
    // 注册module
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(T)
  }

  def getArrayFromJson(jsonStr: String) = {
    JSON.parseArray(jsonStr)
  }

  def getObjectFromJson(jsonStr: String) = {
    JSON.parseObject(jsonStr)
  }

  def main(args: Array[String]) {
    val json = "[{\"batchid\":305322456,\"amount\":20.0,\"count\":20},{\"batchid\":305322488,\"amount\":\"10.0\",\"count\":\"10\"}]"
    val array: JSONArray = JsonUtil.getArrayFromJson(json)
    println(array)
    array.toArray().foreach(json=>{
      println(json)
      val jobj = json.asInstanceOf[JSONObject]
      println(jobj.get("batchid"))
    })

    val jsonStr = "{\"batchid\":119,\"amount\":200.0,\"count\":200}"
    val jsonObj: JSONObject = JsonUtil.getObjectFromJson(jsonStr)
    println(jsonObj)

    val jsonObj2: JSONObject = JsonUtil.getObjectFromJson("{'name':'Wang','age':18,'tag1':[{'tn1':'100','tn2':'101','ts':'ts01'},{'tn1':'100','tn2':'101','ts':'ts02'},{'tn1':'100','tn2':'101','ts':'ts03'}]}")
    println(jsonObj2)


    val jstr = toJsonString(jsonObj2.get("tag1"))
    println(jstr)
  }
}
