package com.sssjd.utils

import java.util

import com.sssjd.configure.LoadConfig
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class HbaseUtil private {

  private val conf = LoadConfig.getHbaseConfiguration()
  private val conn = ConnectionFactory.createConnection(conf)
  private var hTable:Table = _

  registerShutdownHook


  def init(tableName:String): Unit ={
    hTable = conn.getTable(TableName.valueOf(tableName))
  }

  def createTable(tableName:String,cf:String="cf"): Unit ={
    val admin = this.conn.getAdmin
    val userTable = TableName.valueOf(tableName)
    if (admin.tableExists(userTable)){
      println(s"Table $userTable exists!")
    }
    else{
      val tableDesc = new HTableDescriptor(userTable)
      tableDesc.addFamily(new HColumnDescriptor(cf.getBytes))
      admin.createTable(tableDesc)
      println(s"Create table $userTable success!")
    }
  }


  def deleteTable(tableName:String): Unit ={
    val admin = this.conn.getAdmin
    val userTable = TableName.valueOf(tableName)
    if (admin.tableExists(userTable)){
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
      println(s"Table $userTable  deleted!")
    }else{
      println(s"Table $userTable not exists!")
    }
  }

  def put(rowkey:String,cf:String="cf",array:ArrayBuffer[(String,AnyRef)]): Unit ={
    val p = new Put(rowkey.getBytes())
    array.foreach(x => {
      p.addColumn(cf.getBytes,x._1.toString.getBytes,x._2.toString.getBytes)
    })
    hTable.put(p)
  }

  def put(rowkey:String,cf:String,map:com.alibaba.fastjson.JSONObject): Unit ={
    val p = new Put(rowkey.getBytes())
    map.keySet().toArray.foreach(k => {
      val v = map.get(k)
      if(v != null){
        p.addColumn(cf.getBytes,k.toString.getBytes,v.toString.getBytes())
      }
    })
    hTable.put(p)
  }

  def puts(cf:String,df:DataFrame,columns:Array[String] = Array[String]()): Unit ={
    var clos = columns
    if(columns.size<=0){
      clos = df.schema.fieldNames
    }
    val rdd = df.rdd.collect().toList
    var puts = new util.ArrayList[Put]()
    rdd.foreach(row =>{
      val rowKey = row.get(0).toString
      val put = new Put(rowKey.getBytes())
      var idx = 0
      for(col <- clos){
        put.addColumn(cf.getBytes,col.getBytes,row.get(idx).toString.getBytes)
        idx += 1
      }
      puts.add(put)
    })
    hTable.put(puts)
  }


  def getArrData(rowkey:String,cf:String):ArrayBuffer[(String,String)]={
    val get = new Get(Bytes.toBytes(rowkey))
    get.addFamily(Bytes.toBytes(cf))
    val rs = hTable.get(get)
    var result = ArrayBuffer[(String,String)]()
    for (cell <- rs.rawCells()){
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      result.+=((key,value))
    }
    result
  }


  def getMapData(rowKey:String,cf:String):Map[String,String] = {
    val get = new Get(Bytes.toBytes(rowKey))
    get.addFamily(Bytes.toBytes(cf))
    val rs = hTable.get(get)
    var resMap:Map[String,String] = Map()
    for (cell <- rs.rawCells()){
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      resMap += (key -> value)
    }
    resMap
  }


  def getData(rowkeys:Array[String],cf:String,cfs:List[String]=null):List[Map[String,String]]={
    val lstGet = new util.ArrayList[Get]()
    for(rk <- rowkeys){
      val get = new Get(rk.getBytes()).addFamily(cf.getBytes())
      lstGet.add(get)
    }
    val res = hTable.get(lstGet)
    var lstMap = List[Map[String,String]]()
    for(result <- res){
      var resMap: Map[String,String] = Map()
      for(cell <- result.rawCells()){
        val key = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        resMap += (key -> value)
      }
      lstMap = resMap :: lstMap
    }
    lstMap
  }



  def deleteByRowkeyAndCF(rowkey:String,cf:String,key:String): Unit ={
    val del = new Delete(Bytes.toBytes(rowkey))
    del.addColumns(Bytes.toBytes(cf),Bytes.toBytes(key))
    try{
      hTable.delete(del)
    }catch {
      case e:Exception => throw new Exception(s"Failed to delete the HBase table:$e")
    }
  }


  private[this] def registerShutdownHook: Unit ={

    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit ={
        release
      }
    })
  }

  private[this] def release(): Unit ={
    try{
      if(this.hTable != null) this.hTable.close()
      if(this.conn != null) this.conn.close()
    }catch {
      case e:Exception =>println(s"fail to close HbaseClientObj:${e}")
    }
  }

}


object HbaseUtil{

  private val obj = new HbaseUtil()
  def getInstance():HbaseUtil = obj


  def main(args: Array[String]): Unit = {

    val hbaseClient = HbaseUtil.getInstance()
    hbaseClient.createTable("taxi_ns:rapidDetails")
    hbaseClient.init("taxi_ns:rapidDetails")
//    hbaseClient.put(rowkey,"cf",data)
//    hbaseClient.deleteTable("taxi_ns:rapidDetails")

  }

}
