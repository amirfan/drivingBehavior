package com.sssjd.utils

import java.io.FileInputStream
import java.sql
import java.sql.DriverManager
import java.util.Properties
import java.sql.Connection

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object MysqlUtil {



  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
        var table = tableName
        val properties: Properties = getProPerties()
        val prop = new Properties         //配置文件中的key与spark中的key不同,所以创建prop按照spark的格式进行配置数据库
        prop.setProperty("user", properties.getProperty("mysql.username"))
        prop.setProperty("password", properties.getProperty("mysql.password"))
        prop.setProperty("driver", properties.getProperty("mysql.driver"))
        prop.setProperty("url", properties.getProperty("mysql.url"))
        if (saveMode == SaveMode.Overwrite) {
        var conn: Connection = null
        try {
            conn = DriverManager.getConnection(
              prop.getProperty("url"),
              prop.getProperty("user"),
              prop.getProperty("password")
            )
            val stmt = conn.createStatement
            table = table.toUpperCase
            stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
            conn.close()
          }
        catch {
            case e: Exception =>
                println("MySQL Error:")
                e.printStackTrace()
          }
      }
        dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
      }


  def readMysqlTable(spark: SparkSession, tableName: String) = {
       val properties: Properties = getProPerties()
       spark.read
         .format("jdbc")
         .option("url", properties.getProperty("mysql.url"))
         .option("driver", properties.getProperty("mysql.driver"))
         .option("user", properties.getProperty("mysql.username"))
         .option("password", properties.getProperty("mysql.password"))
         //        .option("dbtable", tableName.toUpperCase)
         .option("dbtable", tableName)
         .load()
     }

  def readMysqlTable(spark: SparkSession, table: String, filterCondition: String) = {
     val properties: Properties = getProPerties()
     var tableName = ""
     tableName = "(select * from " + table + " where " + filterCondition + " ) as t1"
     spark.read
       .format("jdbc")
       .option("url", properties.getProperty("mysql.url"))
       .option("driver", properties.getProperty("mysql.driver"))
       .option("user", properties.getProperty("mysql.username"))
       .option("password", properties.getProperty("mysql.password"))
       .option("dbtable", tableName)
       .load()
   }

  def getProPerties() = {
      val properties: Properties = new Properties()
      properties.load(this.getClass().getClassLoader().getResourceAsStream("mysql.properties"))
      properties
      }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("0")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val data = spark.read.option("header", "true").csv("hdfs://192.168.100.177:8020/user/root/VDP_taxi/2017analyse/DZRapidScore/score/2/output/").repartition(1)

    val mysqlDF = data.withColumnRenamed("userId", "jsy")
    mysqlDF.show()

    //保存mysql
    saveASMysqlTable(mysqlDF, "t_score", SaveMode.Append)
    readMysqlTable(spark,"t_score").show()
   }



}
