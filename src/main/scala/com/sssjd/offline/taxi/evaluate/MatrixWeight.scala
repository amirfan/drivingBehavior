package com.sssjd.offline.taxi.evaluate

import breeze.linalg.{Axis, DenseMatrix, sum}

import scala.collection.mutable.ArrayBuffer


/**
  *初始化权重
  */
object MatrixWeight{

  val ue = DenseMatrix((0.5,0.5),(0.5,0.5)) //明细 + 事件
  val we: ArrayBuffer[Double] = calConsistMatrix(ue) //明细 + 事件

  def calWeight()={

    val u0: DenseMatrix[Double] = DenseMatrix((0.5, 0.65,0.75), (0.35, 0.5,0.7),(0.25,0.3,0.5)) //{超速,三急,里程}
    val u1 = DenseMatrix((0.5, 0.4, 0.35),(0.6, 0.5, 0.4),(0.65, 0.6, 0.5))//{超速次数,超速时长,超速等级}
    val u11 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{超速等级A,超速等级B,超速等级C}
    val u2 = DenseMatrix((0.5, 0.6, 0.6), (0.4, 0.5, 0.6), (0.4, 0.4, 0.5)) //{急加速,急减速,急转弯}
    val u3 = DenseMatrix((0.5,0.3),(0.7,0.5)) //载客里程,空驶里程


    val w0 = calConsistMatrix(u0)
    val w1 = calConsistMatrix(u1)
    val w11 = calConsistMatrix(u11)
    val w2 = calConsistMatrix(u2)
    val w3 = calConsistMatrix(u3)
    val wOver: Double = w0(0) * w1(0) //超速次数
    val wLevel= w0(0) * w1(1)        //超速时长
    val wTime = w0(0) * w1(2)        //超速等级
    val wRapidUp = w0(1) * w2(0)     //急加速
    val wRapidDown= w0(1)* w2(1)     //急减速
    val wRapidTurn= w0(1) * w2(2)    //急转弯
    val wMileage = w0(2)
    val weight1 = List(wOver,wLevel,wTime,wRapidUp,wRapidDown,wRapidTurn,wMileage)
    val weight2 = List(w11,w3)
    (weight1,weight2)
  }



  def calEventWeight()={

    val e0  = DenseMatrix((0.5, 0.65), (0.35, 0.5)) //{超速,急加急减速急转弯}
    val e1  = DenseMatrix((0.5, 0.4, 0.35),(0.6, 0.5, 0.4),(0.65, 0.6, 0.5))//{超速次数,超速时长,超速等级}
    val e2  = DenseMatrix((0.5, 0.6, 0.6), (0.4, 0.5, 0.6), (0.4, 0.4, 0.5)) //{急加速,急减速,急转弯}
    val e11 = DenseMatrix((0.5, 0.3),(0.7, 0.5))//{普通超速等级,严重超速等级}

    val ev0 = calConsistMatrix(e0)
    val ev1 = calConsistMatrix(e1)
    val ev2 = calConsistMatrix(e2)
    val ev11 = calConsistMatrix(e11)

    val evOver: Double = ev0(0) * ev1(0) //超速次数
    val evLevel= ev0(0) * ev1(1)         //超速时长
    val evTime = ev0(0) * ev1(2)         //超速等级
    val evRapidUp = ev0(1) * ev2(0)      //急加速
    val evRapidDown= ev0(1)* ev2(1)      //急减速
    val evRapidTurn= ev0(1) * ev2(2)     //急转弯
    val weight1ev = List(evOver,evLevel,evTime,evRapidUp,evRapidDown,evRapidTurn)
    val weight2ev = List(ev11)
    (weight1ev,weight2ev)
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


  def main(args: Array[String]): Unit = {

    println(calWeight())
    println(calEventWeight())

  }
}
