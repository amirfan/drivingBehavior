package com.sssjd.offline.bus.evaluate

import breeze.linalg.{Axis, DenseMatrix, sum}

import scala.collection.mutable.ArrayBuffer

/**
  *初始化权重
  */
object MatrixWeight{

  val ue = DenseMatrix((0.5,0.5),(0.5,0.5)) //明细 + 事件
  val we: ArrayBuffer[Double] = calConsistMatrix(ue) //明细 + 事件



  def calWeight()={

    val u0: DenseMatrix[Double] = DenseMatrix((0.5, 0.65), (0.35, 0.5)) //{超速,急加急减速急转弯}
    val u1 = DenseMatrix((0.5, 0.4, 0.35),(0.6, 0.5, 0.4),(0.65, 0.6, 0.5))//{超速次数,超速时长,超速等级}
    val u2 = DenseMatrix((0.5, 0.6, 0.6), (0.4, 0.5, 0.6), (0.4, 0.4, 0.5)) //{急加速,急减速,急转弯}
    val u11 = DenseMatrix((0.5, 0.4, 0.3),(0.6, 0.5, 0.35),(0.7, 0.65, 0.5))//{路段超速次数,站点超速次数,路口超速次数}
    val u13 = DenseMatrix((0.5, 0.4, 0.3),(0.6, 0.5, 0.35),(0.7, 0.65, 0.5))//{路段超速等级,站点超速等级,路口超速等级}
    val u131 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{路段等级1,路段等级2,路段等级3}
    val u132 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{站点等级1,站点等级2,站点等级3}
    val u133 = DenseMatrix((0.5, 0.3, 0.2),(0.7, 0.5, 0.3),(0.8, 0.7, 0.5))//{路口等级1,路口等级2,路口等级3}
    val w0 = calConsistMatrix(u0)
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



  def calEventWeight()={

    val e0  = DenseMatrix((0.5, 0.65), (0.35, 0.5)) //{超速,急加急减速急转弯}
    val e1  = DenseMatrix((0.5, 0.4, 0.35),(0.6, 0.5, 0.4),(0.65, 0.6, 0.5))//{超速次数,超速时长,超速等级}
    val e2  = DenseMatrix((0.5, 0.6, 0.6), (0.4, 0.5, 0.6), (0.4, 0.4, 0.5)) //{急加速,急减速,急转弯}
    val e11 = DenseMatrix((0.5, 0.4, 0.3),(0.6, 0.5, 0.35),(0.7, 0.65, 0.5))//{路段超速次数,站点超速次数,路口超速次数}
    val e13 = DenseMatrix((0.5, 0.4, 0.3),(0.6, 0.5, 0.35),(0.7, 0.65, 0.5))//{路段超速等级,站点超速等级,路口超速等级}
    val e131 = DenseMatrix((0.5, 0.3),(0.7, 0.5))//{路段等级B,路段等级C}
    val e132 = DenseMatrix((0.5, 0.3),(0.7, 0.5))//{站点等级B,站点等级C}
    val e133 = DenseMatrix((0.5, 0.3),(0.7, 0.5))//{路口等级B,路口等级C}
    val ev0 = calConsistMatrix(e0)
    val ev1 = calConsistMatrix(e1)
    val ev2 = calConsistMatrix(e2)
    val ev11 = calConsistMatrix(e11)
    val ev13 = calConsistMatrix(e13)
    val ev131 = calConsistMatrix(e131)
    val ev132 = calConsistMatrix(e132)
    val ev133 = calConsistMatrix(e133)
    val evOver: Double = ev0(0) * ev1(0)
    val evLevel= ev0(0) * ev1(1)
    val evTime = ev0(0) * ev1(2)
    val evRapidUp = ev0(1) * ev2(0)
    val evRapidDown= ev0(1)* ev2(1)
    val evRapidTurn= ev0(1) * ev2(2)
    val weight1ev = List(evOver,evLevel,evTime,evRapidUp,evRapidDown,evRapidTurn)
    val weight2ev = List(ev11,ev13,ev131,ev132,ev133)
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
