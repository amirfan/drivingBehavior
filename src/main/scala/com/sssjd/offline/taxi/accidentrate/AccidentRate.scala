package com.sssjd.offline.taxi.accidentrate

import com.sssjd.configure.LoadConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object AccidentRate {


  System.setProperty("HADOOP_USER_NAME", LoadConfig.getHdfsUser())

  def main(args: Array[String]): Unit = {

    val fsPath = LoadConfig.getHdfsTaxiCongig()

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TaxiEvaluate")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    import spark.implicits._

    for(i <- Array(1,2)) {

      val score: Dataset[Row] = spark.read.option("header", "true").csv(fsPath + s"DZRapidScore/score/$i/output/").repartition(1)
      val accident: Dataset[Row] = spark.read.option("header", "true").csv(fsPath + s"DZRapidScore/accident/").repartition(1)

      score.show()
      accident.show()


//      feature.write.mode(SaveMode.Overwrite).option("user", LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath + s"DZRapidScore/score/$i/feature/")
    }



    }

}
