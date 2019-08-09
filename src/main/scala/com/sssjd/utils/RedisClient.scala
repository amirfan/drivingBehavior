package com.sssjd.utils

import com.sssjd.configure.PropertiesLoader
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by amir.fan on 2019/8/9.
  */
object RedisClient extends Serializable {

  val redisHost = PropertiesLoader.load("redis.host")

  val redisPort = PropertiesLoader.load("redis.port").toInt

  val redisTimeout = PropertiesLoader.load("redis.timeout").toInt

  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

}