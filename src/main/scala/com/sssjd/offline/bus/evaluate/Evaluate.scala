package com.sssjd.offline.bus.evaluate

import com.sssjd.configure.LoadConfig

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object Evaluate {

  val input = "JRInput"
  val output = "JRScore"

  case class DriverScore(userId:String,overSpeedCountScore:Double,overSpeedLevelScore:Double,overSpeedTimeScore:Double,rapidUpScore:Double,rapidDownScore:Double,rapidTurnScore:Double)
  case class DriverEventScore(userId:String,overSpeedEventCountScore:Double,overSpeedEventLevelScore:Double,overSpeedEventTimeScore:Double,rapidUpEventScore:Double,rapidDownEventScore:Double,rapidTurnEventScore:Double)

  System.setProperty("HADOOP_USER_NAME", LoadConfig.getHdfsUser())

  def main(args: Array[String]): Unit = {

    val fsPath = LoadConfig.getHdfsConfiguration()

    val spark = SparkSession.builder()
              .master("local[*]")
      .appName("busEvaluate")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val weight = MatrixWeight.calWeight()
    val eventWeight = MatrixWeight.calEventWeight()


    val feature: DataFrame = spark.read.option("header", "true").csv(fsPath+s"JR_451_Data/score/$input").repartition(1)


    //计算明细分
    val scoreDetails: Dataset[Features.DriverBehavior] = Features.calFeature(feature, weight)
    val calQ = Score.calQuantiles(scoreDetails)

    val qu: String => Array[Double] = (col: String) => calQ(col)

    val evalDetailScore: Dataset[DriverScore] = scoreDetails.map(e => {
        DriverScore(e.uid,
          Score.getScore(e.overCnt, qu("overCnt")),
          Score.getScore(e.overLevel, qu("overLevel")),
          Score.getScore(e.sectionTime, qu("sectionTime")),
          Score.getScore(e.rapidUpCount, qu("rapidUpCount")),
          Score.getScore(e.rapidDownCount, qu("rapidDownCount")),
          Score.getScore(e.rapidTurnCount, qu("rapidTurnCount")))
      })


    //计算事件分
    val scoreEvents: Dataset[Features.DriverBehaviorEvent] = Features.calEventFeature(feature, eventWeight)
    val calEventQ = Score.calEventQuantiles(scoreEvents)
    val quEvent:String => Array[Double] = {
        col => calEventQ(col)
      }
    val evalEventScore: Dataset[DriverEventScore] = scoreEvents.map(e => {
        DriverEventScore(e.uid,
          Score.getScore(e.overEventCnt, quEvent("overEventCnt")),
          Score.getScore(e.overEventLevel, quEvent("overEventLevel")),
          Score.getScore(e.sectionEventTime, quEvent("sectionEventTime")),
          Score.getScore(e.rapidUpEventCount, quEvent("rapidUpEventCount")),
          Score.getScore(e.rapidDownEventCount, quEvent("rapidDownEventCount")),
          Score.getScore(e.rapidTurnEventCount, quEvent("rapidTurnEventCount")))
      })



    val scoreDetailsDF = evalDetailScore.toDF().createOrReplaceTempView("scoreDetails")
    val scoreEventsDF = evalEventScore.toDF().createOrReplaceTempView("scoreEvents")

    val sql =
      s"""
         |select details.userId,
         |0.7 * overSpeedCountScore + 0.3* overSpeedEventCountScore as overScore,
         |0.7 * overSpeedLevelScore + 0.3*overSpeedEventLevelScore as LevelScore,
         |0.7 * overSpeedTimeScore + 0.3*overSpeedEventTimeScore as TimeScore,
         |0.7 * rapidUpScore + 0.3*rapidUpEventScore as rapidUpScore,
         |0.7 * rapidDownScore + 0.3*rapidDownEventScore as rapidDownScore,
         |0.7 * rapidTurnScore + 0.3*rapidTurnEventScore as rapidTurnScore
         |from scoreDetails as details
         |join scoreEvents  as event
         |on details.userId = event.userId
       """.stripMargin
    val scoreInput = spark.sql(sql)

    scoreInput.show()

    val udfSum = (overScore: Double, LevelScore: Double, TimeScore: Double, rapidUpScore: Double, rapidDownScore: Double, rapidTurnScore: Double) => {
        val over = if (overScore==100)  95*weight._1(0)  else overScore * weight._1(0)
        val level = if (LevelScore==100)  95*weight._1(1) else LevelScore * weight._1(1)
        val time = if (TimeScore==100)  95*weight._1(2) else TimeScore * weight._1(2)
        val rapidUp = if (rapidUpScore==100)  95*weight._1(3) else rapidUpScore * weight._1(3)
        val rapidDown = if (rapidDownScore==100)  95*weight._1(4) else rapidDownScore * weight._1(4)
        val rapidTurn = if (rapidTurnScore==100)  95*weight._1(5) else rapidTurnScore * weight._1(5)
        over + level + time + rapidUp + rapidDown + rapidTurn
      }
    val Sum = udf(udfSum)

    val evalDS = scoreInput.withColumn("comprehensiveScore",
                                        Sum($"overScore", $"LevelScore", $"TimeScore", $"rapidUpScore", $"rapidDownScore", $"rapidTurnScore"))

    evalDS.show()

    evalDS.write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath+s"JR_451_Data/score/$output")
    }

}
