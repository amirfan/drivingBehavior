package com.sssjd.configure


import java.net.InetAddress

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.mutable
import scala.io.Source

object LoadConfig {

  val config = getConfigs()


  def getHdfsConfiguration(): String = config.get("fsGJPath").getOrElse().toString
  def getHdfsTaxiCongig(): String = config.get("fsCZPath").getOrElse().toString


  def getHdfsUser(): String = {
    config.get("HADOOP_USER_NAME").getOrElse().toString
  }


  def getHbaseConfiguration(): Configuration ={

    val hbaseConf = HBaseConfiguration.create()
    val quorum = config.get("hbase.zookeeper.quorum").getOrElse().toString
    val port = config.getOrElse("hbase.zookeeper.property.clientPort","")
    hbaseConf.set("hbase.zookeeper.quorum",quorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort",port)
    hbaseConf.set("hbase.client.keyvalue.maxsize","524288000")
    hbaseConf
  }



  def getRedis()={
    config.get("redis.host").getOrElse().toString
  }

  def getSqlServerConfig() ={
    val map = new mutable.HashMap[String,String]()
    map.put("jdbcDriver",config.get("jdbcDriver").getOrElse().toString)
    map.put("jdbcSize",config.get("jdbcSize").getOrElse().toString)
    map.put("taxi_connectionUrl",config.get("taxi_connectionUrl").getOrElse().toString)
    map.put("bus_connectionUrl",config.get("bus_connectionUrl").getOrElse().toString)
    map
  }




  def getKafkaConfig()={

    val map = new mutable.HashMap[String,String]()
    map.put("brokers_taxi",config.get("brokers_taxi").getOrElse().toString)
    map.put("brokers_bus",config.get("brokers_bus").getOrElse().toString)
    map.put("topic_taxi_roadmatch",config.get("topic_taxi_roadmatch").getOrElse().toString)
    map.put("topic_taxi_score",config.get("topic_taxi_score").getOrElse().toString)
    map.put("topic_taxi_rapid",config.get("topic_taxi_rapid").getOrElse().toString)
    map.put("topic_bus_roadmatch",config.get("topic_bus_roadmatch").getOrElse().toString)
    map.put("topic_bus_score",config.get("topic_bus_score").getOrElse().toString)
    map.put("topic_bus_rapid",config.get("topic_bus_rapid").getOrElse().toString)
    map.put("vdp_ridestatus",config.get("vdp_ridestatus").getOrElse().toString)
    map
  }



  def getConfigs():Map[String, String] ={

    val ip = getIp()
    val configFile = ip match {
      case _ if ip.startsWith(IpEnum.DevelopEnv.toString) =>  "develop_info.properties"
      case _ if ip.startsWith(IpEnum.awsEnv.toString) => "aws_info.properties"
    }
    val s = Source.fromInputStream(
      this.getClass.getClassLoader.getResource(configFile).openStream()
    ).getLines().filter(_.contains("=")).filterNot(_.startsWith("#")).map(_.split("=")).map(x=>{
      (x(0),x(1))
    }).toMap
    s
  }

  def getIp():String={
    InetAddress.getLocalHost().getHostAddress()
  }

  def main(args: Array[String]): Unit = {
    val s = getConfigs()

    println(s)
//   val k =  getKafkaConfig()
//    val r =getRedis()
//    println(k)
//    println(r)
  }

}
