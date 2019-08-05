package com.sssjd.aws.online.taxi

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis
import java.text.SimpleDateFormat
import java.util.Properties

import breeze.numerics.abs
import com.sssjd.configure.LoadConfig
import com.sssjd.utils.RedisUtil.{getJedis, retJedis}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object rapidSpeed {
  //0.661721	0.687157	0.712594	0.738031	0.763468
  val acceThreshold = 0.763468
  val deceThreshold = -0.763468
  val speedLimit = 20.5
  val angleLimit = 11.5
//  val turnLimit = 34.5
  val kafkaConf = LoadConfig.getKafkaConfig()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rapidStreaming")
//      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    val KafkaParams=Map[String,String](
      "bootstrap.servers"->kafkaConf.get("brokers_taxi").getOrElse().toString,
      "group.id"->"rapidTaxiConsumer",
      "auto.offset.reset"->"latest",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val topics=List(kafkaConf.get("topic_taxi_roadmatch").getOrElse().toString)
    val message = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics,KafkaParams))
    message.foreachRDD(v=>fun(v,spark))
    ssc.start()
    ssc.awaitTermination()
  }


  def kafkaProducer(msg:String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers",kafkaConf.get("brokers_taxi").getOrElse().toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val record = new ProducerRecord[String,String](kafkaConf.get("topic_taxi_rapid").getOrElse().toString, msg)
    println(record)
    producer.send(record)
    producer.close()
  }


  /**
    * 急加速/急减速/急转弯
    * preList(0):时间pretime  preList(1):速度  preList(2):方向
    * curList(2):时间curtime  curList(5):速度  curList(6):方向
    * taxiPreGps存储上一个GPS信息 K(jsy)->V(时间,速度,角度)
    * taxiPreStatus记录上一时刻三急状态(0/1/2/3)
    * taxiRapidCnt记录评分区间三急数目
    *
    */


  def fun(v:RDD[ConsumerRecord[String,String]],spark:SparkSession): Unit ={
    if(!v.isEmpty()){
      val jedis: Jedis = getJedis()
      try{
        val rdd:RDD[String] = v.map(_.value())
        val array = rdd.collect()
        for (ary <- array){
          val curList= ary.split(",").toList
          var preList = List[String]()
          try {
            preList = jedis.hget("taxiPreGps",curList.head).split(",").toList
          }
          catch {
            case e: Exception =>
          }
          jedis.hset("taxiPreGps", curList.head,curList(3).+(",").+(curList(2).+(",").+(curList(8))))


          if (preList.nonEmpty && preList.head!=curList(3)){

            val pretime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(preList.head).getTime
            val curtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(curList(3)).getTime

            val timeInterval: Long = (curtime - pretime)/1000
            val sudSpeed = (curList(2).toFloat - preList(1).toFloat)/(timeInterval*3.6)
            val avgSpeed = (curList(2).toFloat + preList(1).toFloat)/2
            val angle = abs(curList(8).toFloat - preList(2).toFloat)
            val angSpeed = angle match {
              case angle if (angle>180.0) => (360.0 - angle)/timeInterval
              case _ => angle/timeInterval
            }

            var backList =
              if (sudSpeed > acceThreshold && curList(2).toFloat>speedLimit)    curList.+:("1")
              else if (sudSpeed < deceThreshold && preList(1).toFloat>speedLimit)    curList.+:("2")
              else if (angSpeed >angleLimit && avgSpeed >speedLimit)  curList.+:("3")
              else curList.+:("0")

            val status = jedis.hget("taxiPreStatus",curList.head)
            if (backList.head==status) backList = curList.+:("0")
            else jedis.hset("taxiPreStatus",curList.head,backList.head)

            if(backList.head!="0"){
              val msg = backList(1).+(",").+(backList(2)).+(",").+(backList(3)).+(",").+(backList(4)).+(",").+(backList(5)).+(",").+(backList(6)).+(",").+(backList(7)).+(",").+(backList(0))
              var mse = jedis.hget("taxiRapidCnt",backList(1))
              if(mse==null){
                mse = backList.head
              }else{
                mse = mse.+(",").+(backList.head)
              }

              jedis.hset("taxiRapidCnt",backList(1),mse)

              kafkaProducer(msg)
            }
          }
        }
      }
      catch {
        case e:Exception =>e.printStackTrace()
      }
      finally{
        retJedis(jedis)
      }
    }
  }


}
