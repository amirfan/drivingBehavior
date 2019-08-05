package com.sssjd.aws.online.taxi

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}
import java.sql.Timestamp

import com.sssjd.aws.online.taxi.rapidIdentification_old.dateFormat
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object kafkaConsumer extends App {
  val props = new Properties()
  val topic = "vdp_roadmatch"
  props.put("bootstrap.servers","cdh177:9092,cdh11:9092,cdh204:9092")
  props.put("client.id","ScalaConsumerExample")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "consumer_group_id")
  val consumer = new KafkaConsumer[String,String](props)
  consumer.subscribe(util.Collections.singleton(topic))
  val records = consumer.poll(10000)
  println(records.count())
  println(records.isEmpty)

  for (record <- records.asScala){
    println(record)
    println("---------------")
  }
  consumer.close()
}