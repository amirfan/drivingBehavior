package com.sssjd.utils
import com.sssjd.configure.LoadConfig
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  val config: JedisPoolConfig = new JedisPoolConfig
  var pool:JedisPool = null
  init()


  def init(): Unit = {
    config.setMaxWaitMillis(2000)
    import redis.clients.jedis.JedisPool
    pool = new JedisPool(config, LoadConfig.getRedis(), 6379, 2000)
  }


  def getJedis():Jedis= {
    pool.getResource()
  }



  def retJedis(jds:Jedis): Unit = {
    if (jds != null) {
      //      pool.returnResource(jds)
      jds.close()
    }
  }


  def main(args: Array[String]) {

    val jedis: Jedis = getJedis()
//    jedis.hset("busEHstatus","field","null:3:2:2:2:1")
    val stat = jedis.hget("rapidStatTaxi","stat")
//    jedis.del("taxiPreStatus")
//    jedis.del("busRapidCnt")
//    jedis.del("busScore")
//    jedis.del("busPreGps")
    println(stat)
    val js = JsonUtil.getObjectFromJson(stat)
    val s = js.get("cnt").toString.toDouble
    println(s)
    retJedis(jedis)

  }

}
