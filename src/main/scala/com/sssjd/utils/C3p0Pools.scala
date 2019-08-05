package com.sssjd.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource


object C3p0Pools {

  private var dataSource: ComboPooledDataSource = _
  init()
  registerShutdownHook()


  private def init(): Unit = {

    try {
      dataSource = new ComboPooledDataSource()
      val prop = new Properties()
      val in = this.getClass.getClassLoader.getResourceAsStream("c3p0.properties")
      prop.load(in)
      in.close()

      dataSource.setDriverClass(prop.getProperty("c3p0.driverClass"))
      dataSource.setJdbcUrl(prop.getProperty("c3p0.jdbcUrl"))
      dataSource.setUser(prop.getProperty("c3p0.user"))
      dataSource.setPassword(prop.getProperty("c3p0.password"))
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def getConnection(isAuto: Boolean = true): Connection = {

    try {
//      println(dataSource)
      val conn = dataSource.getConnection
      conn.setAutoCommit(isAuto)
      conn
    } catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }


  def query(conn: Connection, sql: String, params: Array[Any] = null): ResultSet = {

    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      pstmt = conn.prepareStatement(sql)

//      if (pstmt != null) {
//        (0 until params.length).foreach(
//          i => {
//            pstmt.setObject(i + 1, params(i))
//          }
//        )
//      }
      rs = pstmt.executeQuery()
      rs
    } catch {
      case e: Exception => e.printStackTrace()
        throw new Exception
    }
  }

  def execute(sql: String, params: Array[Any], outConn: Connection = null): Int = {

    var conn = outConn
    if (conn == null) {
      conn = getConnection()
    }

    var cont = 0
    try {
      var psm: PreparedStatement = conn.prepareStatement(sql)
      if (params != null) {
        for (i <- 0 to params.size - 1)
          psm.setObject(i + 1, params(i))
      }

      cont = psm.executeUpdate()
      psm.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new Exception
      }
    } finally {
      if (conn != null && conn.getAutoCommit) {
        conn.close()
      }
    }
    cont
  }

  private[this] def registerShutdownHook(): Unit = {

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        try {
          println("C3p0Pools release ....")
          dataSource.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }

  def main(args: Array[String]): Unit = {

    for (i <- 0 to 10) {

      val conn = getConnection()
      conn.setAutoCommit(false)

      val sql =
        """
          |select * from t_newbuslocation limit 8
        """.stripMargin
      val res: ResultSet = query(conn,sql)
      while (res.next()) {
        println(res.getString(1))
      }
      conn.close()
    }


  }
}
