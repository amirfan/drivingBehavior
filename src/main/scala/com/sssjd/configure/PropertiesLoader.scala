package com.sssjd.configure

import java.net.InetAddress
import java.util.Properties

import scala.io.Source

object PropertiesLoader {

  def getIp():String={
    InetAddress.getLocalHost().getHostAddress()
  }

  def getConfigFile() ={
    val ip = getIp()
    val configFile = ip match {
      case _ if ip.startsWith(IpEnum.DevelopEnv.toString) =>  "develop_info.properties"
      case _ if ip.startsWith(IpEnum.awsEnv.toString) => "aws_info.properties"
    }
    configFile
  }

  def getProPerties() = {
    val configfile = getConfigFile()
    val properties: Properties = new Properties()
    properties.load(this.getClass().getClassLoader().getResourceAsStream(configfile))
    properties
  }

  def load(key:String) ={
    val configProperties = getProPerties()
    configProperties.getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    val s = PropertiesLoader.load("redis")
    println(s)
  }

}
