package com.sssjd.offline.taxi.evaluate

import org.apache.spark.sql.Dataset

/**
  * 设置评分区间
  */

object Score {

  val getScore = (rw: Double, st:Array[Double])=> {
    if (rw == 0d) {
      100
    } else if (rw >= st(0) && rw <= st(1)) {
      val scoreBase = 80
      val scoreMax = 95
      scoreBase + (st(1) - rw) / (st(1) - st(0) * (scoreMax - scoreBase))
    } else if (rw >= st(1) && rw <= st(2)) {
      val scoreBase = 75
      val scoreMax = 80
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

  def calQuantiles(ds: Dataset[Features.DriverBehavior]): Map[String, Array[Double]] = {
    val array: Array[(String, Array[Double])] = for {
      col <- ds.columns
      if col != "uid"
      st = ds.stat.approxQuantile(col, Array(0, 0.05, 0.2, 0.45, 0.75, 0.9, 1), 0)
    } yield {
      col -> st
    }
    array.toMap
  }


  def calEventQuantiles(ds: Dataset[Features.DriverBehaviorEvent]): Map[String, Array[Double]] = {
    val array: Array[(String, Array[Double])] = for {
      col <- ds.columns
      if col != "uid"
      st = ds.stat.approxQuantile(col, Array(0, 0.05, 0.2, 0.45, 0.75, 0.9, 1), 0)
    } yield {
      col -> st
    }
    array.toMap
  }




}

