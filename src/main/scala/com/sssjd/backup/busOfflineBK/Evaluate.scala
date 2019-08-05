package com.sssjd.backup.busOfflineBK

import breeze.linalg.{Axis, DenseMatrix, sum}
import com.sssjd.configure.LoadConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

case class DriverBehavior(uid:String,overCnt:Double,overLevel:Double,sectionTime:Double,rapidUpCount:Double,rapidDownCount:Double,rapidTurnCount:Double)
case class DriverScore(userId:String,overSpeedCountScore:Double,overSpeedLevelScore:Double,overSpeedTimeScore:Double,rapidUpScore:Double,rapidDownScore:Double,rapidTurnScore:Double)


/**
  *初始化权重
  */
object MatrixWeight{

  val u0: DenseMatrix[Double] = DenseMatrix((0.5, 0.65), (0.35, 0.5))
  //{超速,急加急减速急转弯}
  val u1 = DenseMatrix((0.5, 0.4, 0.35),(0.6, 0.5, 0.4),(0.65, 0.6, 0.5))//{超速次数,超速时长,超速等级}
  val u2 = DenseMatrix((0.5, 0.6, 0.6), (0.4, 0.5, 0.6), (0.4, 0.4, 0.5))
  //{急加速,急减速,急转弯}
  val u11 = DenseMatrix((0.5, 0.4, 0.3),(0.6, 0.5, 0.35),(0.7, 0.65, 0.5))//{路段超速次数,站点超速次数,路口超速次数}
  val u13 = DenseMatrix((0.5, 0.4, 0.3),(0.6, 0.5, 0.35),(0.7, 0.65, 0.5))//{路段超速等级,站点超速等级,路口超速等级}
  val u131 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{路段等级1,路段等级2,路段等级3}
  val u132 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{站点等级1,站点等级2,站点等级3}
  val u133 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{路口等级1,路口等级2,路口等级3}

  def calWeight()={

    val w0: ArrayBuffer[Double] = calConsistMatrix(u0)
    val w1 = calConsistMatrix(u1)
    val w2 = calConsistMatrix(u2)
    val w11 = calConsistMatrix(u11)
    val w13 = calConsistMatrix(u13)
    val w131 = calConsistMatrix(u131)
    val w132 = calConsistMatrix(u132)
    val w133 = calConsistMatrix(u133)
    val wOver: Double = w0(0) * w1(0)
    val wLevel= w0(0) * w1(1)
    val wTime = w0(0) * w1(2)
    val wRapidUp = w0(1) * w2(0)
    val wRapidDown= w0(1)* w2(1)
    val wRapidTurn= w0(1) * w2(2)
    val weight1 = List(wOver,wLevel,wTime,wRapidUp,wRapidDown,wRapidTurn)
    val weight2 = List(w11,w13,w131,w132,w133)
    (weight1,weight2)
  }

  def calConsistMatrix(m:DenseMatrix[Double]):ArrayBuffer[Double]={
    val r = sum(m, Axis._1)
    val mx = DenseMatrix.ones[Double](m.rows,m.cols)
    for(i <- 0 until m.rows){
      for(j <- 0 until m.cols){
        mx(i,j) = (r(i) - r(j))/(2 * (m.rows -1))+0.5
      }
    }
    calSortWeight(mx)
  }

  def calSortWeight(matrix: DenseMatrix[Double]):ArrayBuffer[Double] = {
    val r = sum(matrix, Axis._1)
    val w = ArrayBuffer[Double]()
    val m = matrix.rows
    for (i <- 0 until m) {
      val wi = 2.0 /(m * (m - 1)) * r(i) + 1.0 / m - 1.0 / (m - 1)
      w.append(wi)
    }
    w
  }
}

/**
  * 计算特征项
  * 根据HDFS原始特征统计数据
  */

object Features{

  def calFeature(feature: DataFrame,weight: (List[Double], List[ArrayBuffer[Double]])):Dataset[DriverBehavior]={
    val scoreDetails: Dataset[DriverBehavior] = feature.map(row=>{
      val uid = row.getAs[String](0)
      val sectionCount = row.getAs[String](2).toDouble/row.getAs[String](1).toDouble
      val crossCount = row.getAs[String](7).toDouble/row.getAs[String](1).toDouble
      val stationCount = row.getAs[String](11).toDouble/row.getAs[String](1).toDouble
      val overCnt = weight._2(0)(0) * sectionCount + weight._2(0)(1)*crossCount + weight._2(0)(2)*stationCount
      val sectionACount  = row.getAs[String](4).toDouble/row.getAs[String](1).toDouble
      val sectionBCount  = row.getAs[String](5).toDouble/row.getAs[String](1).toDouble
      val sectionCCount  = row.getAs[String](6).toDouble/row.getAs[String](1).toDouble
      val sectionLevel = weight._2(2)(0) * sectionACount + weight._2(2)(1) * sectionBCount + weight._2(2)(2) * sectionCCount
      val crossACount  = row.getAs[String](8).toDouble/row.getAs[String](1).toDouble
      val crossBCount  = row.getAs[String](9).toDouble/row.getAs[String](1).toDouble
      val crossCCount  = row.getAs[String](10).toDouble/row.getAs[String](1).toDouble
      val crossLevel = weight._2(4)(0) * crossACount + weight._2(4)(1) * crossBCount + weight._2(4)(2) * crossCCount
      val stationACount  = row.getAs[String](8).toDouble/row.getAs[String](1).toDouble
      val stationBCount  = row.getAs[String](9).toDouble/row.getAs[String](1).toDouble
      val stationCCount  = row.getAs[String](10).toDouble/row.getAs[String](1).toDouble
      val stationLevel = weight._2(3)(0) * stationACount + weight._2(3)(0) * stationBCount + weight._2(3)(0) * stationCCount
      val overLevel =  weight._2(1)(0) * sectionLevel + weight._2(1)(1) * crossLevel + weight._2(1)(2) * stationLevel
      val sectionTime = row.getAs[String](3).toDouble/60
      val rapidUpCount = row.getAs[String](15).toDouble/row.getAs[String](1).toDouble
      val rapidDownCount =row.getAs[String](16).toDouble/row.getAs[String](1).toDouble
      val rapdTurnCount =row.getAs[String](17).toDouble/row.getAs[String](1).toDouble
      DriverBehavior(uid,overCnt,overLevel,sectionTime,rapidUpCount,rapidDownCount,rapdTurnCount)
    })(Encoders.product[DriverBehavior])
    scoreDetails
  }
}

/**
  * 设置评分区间
  */

object Score {

  val getScore = (rw: Double, st:Array[Double])=> {
    if (rw == 0d) {
      100
    } else if (rw >= st(0) && rw <= st(1)) {
      val scoreBase = 85
      val scoreMax = 95
      scoreBase + (st(1) - rw) / (st(1) - st(0) * (scoreMax - scoreBase))
    } else if (rw >= st(1) && rw <= st(2)) {
      val scoreBase = 75
      val scoreMax = 85
      scoreBase + (st(2) - rw) / (st(2) - st(1) * (scoreMax - scoreBase))

    } else if (rw >= st(2) && rw <= st(3)) {
      val scoreBase = 70
      val scoreMax = 75
      scoreBase + (st(3) - rw) / (st(3) - st(2) * (scoreMax - scoreBase))

    } else if (rw >= st(3) && rw <= st(4)) {
      val scoreBase = 60
      val scoreMax = 70
      scoreBase + (st(4) - rw) / (st(4) - st(3) * (scoreMax - scoreBase))

    } else if (rw >= st(4) && rw <= st(5)) {
      val scoreBase = 50
      val scoreMax = 60
      scoreBase + (st(5) - rw) / (st(5) - st(4) * (scoreMax - scoreBase))

    } else {
      val scoreBase = 35
      val scoreMax = 50
      scoreBase + (st(6) - rw) / (st(6) - st(5) * (scoreMax - scoreBase))
    }
  }

  def calQuantiles(ds: Dataset[DriverBehavior]): Map[String, Array[Double]] = {
    val array: Array[(String, Array[Double])] = for {
      col <- ds.columns
      if col != "uid"
      st = ds.stat.approxQuantile(col, Array(0, 0.05, 0.3, 0.5, 0.8, 0.95, 1), 0)
    } yield {
      col -> st
    }
    array.toMap
  }
}

/**
  *评分
  */

object Evaluate {

    System.setProperty("HADOOP_USER_NAME", LoadConfig.getHdfsUser())

    def main(args: Array[String]): Unit = {

      val fsPath = LoadConfig.getHdfsConfiguration()

      val spark = SparkSession.builder()
//        .master("local[*]")
        .appName("busEvaluate")
        .getOrCreate()
      val sc = spark.sparkContext
      import spark.implicits._

      val weight = MatrixWeight.calWeight()
      for (mon <- 0 to 10) {
        val feature: DataFrame = spark.read.option("header", "true").csv(fsPath+"report/"+mon+"/scoreInput").repartition(1)

        val scoreDetails = Features.calFeature(feature, weight)
        val calQ = Score.calQuantiles(scoreDetails)
        val qu: String => Array[Double] = (col: String) => calQ(col)

        val evalScore: Dataset[DriverScore] = scoreDetails.map(e => {
          DriverScore(e.uid,
            Score.getScore(e.overCnt, qu("overCnt")),
            Score.getScore(e.overLevel, qu("overLevel")),
            Score.getScore(e.sectionTime, qu("sectionTime")),
            Score.getScore(e.rapidUpCount, qu("rapidUpCount")),
            Score.getScore(e.rapidDownCount, qu("rapidDownCount")),
            Score.getScore(e.rapidTurnCount, qu("rapidTurnCount")))
        })

        val udfSum = (overScore: Double, LevelScore: Double, TimeScore: Double, rapidUpScore: Double, rapidDownScore: Double, rapidTurnScore: Double) => {
          val over = overScore * weight._1(0)
          val level = LevelScore * weight._1(1)
          val time = TimeScore * weight._1(2)
          val rapidUp = rapidUpScore * weight._1(3)
          val rapidDown = rapidDownScore * weight._1(4)
          val rapidTurn = rapidTurnScore * weight._1(5)
          over + level + time + rapidUp + rapidDown + rapidTurn
        }
        val Sum = udf(udfSum)


        val evalDS = evalScore.withColumn("comprehensiveScore", Sum($"overSpeedCountScore", $"overSpeedLevelScore", $"overSpeedTimeScore", $"rapidUpScore", $"rapidDownScore", $"rapidTurnScore"))
        evalDS.write.mode(SaveMode.Overwrite).option("user",LoadConfig.getHdfsUser()).option("header", "true").csv(fsPath+"report/"+mon+"/score")
      }
    }
}