import java.text.SimpleDateFormat

import org.apache.spark
import org.apache.spark.sql.SparkSession

object merge {

  val fm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val timeStamp = (tm:String) =>{
    val dt = fm.parse(tm)
    dt.getTime()
  }




  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()

      .appName("rapidtest")
       .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    spark.udf.register( "timeStamp" , timeStamp )

//
//    val df = spark.read.option("encoding", "utf8").option("header", "true").csv("F:\\三急标准/rapid01")
//
//    df.repartition(1).write.option("header", "true").csv("F:\\三急标准/rpd1/")
    val s = "2019-06-01 08:45:07"


    println(timeStamp(s))



  }

}
