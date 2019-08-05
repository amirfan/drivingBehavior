package com.sssjd.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.collection.mutable.ArrayBuffer

object SqlserverUtil {

  val jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val jdbcSize = 1
  val connectionUrl ="jdbc:sqlserver://192.168.100.97:1500;databaseName=JDTaxiDB;user=sa;password=sss_abc_123"

  var datasource = ArrayBuffer[Connection]()
  var ps:PreparedStatement =null
  var rs:ResultSet=null

//  init()
  Class.forName(jdbcDriver).newInstance()
  registerShutdownHook()


  private def init(): Unit ={
    try{

      (0 until jdbcSize).foreach(i=>{
        try{
          val connection = DriverManager.getConnection(connectionUrl)
          datasource += connection
          None
        }catch {
          case e:Exception => e.printStackTrace()
            throw new Exception
        }
      })
    }catch {
      case e:Exception => e.printStackTrace()
        throw new Exception
    }
  }




  private[this] def createConnection(): Unit = synchronized{
    try{
      val conn = DriverManager.getConnection( connectionUrl )
      datasource += conn
    }catch{
      case e:Exception =>{
        e.printStackTrace
        throw new Exception
      }
    }
  }


  private[this] def getConnection():Connection = synchronized {
    createConnection()
    while( datasource.length == 0 ){
      try{
        Thread.sleep( 10 )
      }catch{
        case e:Exception => {
          e.printStackTrace
          throw new Exception
        }
      }
    }

    var index = -1
    import scala.util.control.Breaks._
    breakable{
      (0 until datasource.length ).foreach{
        i => {
          if( !datasource(i).isClosed ){
            index = i
            break
          }
        }
      }
    }

    if( index equals( -1 ) ){
      datasource.remove( 0 )
      createConnection()
      index = datasource.length-1
    }
    datasource.remove( index )
  }


  def closeConnection( connection:Connection ) : Unit = synchronized{
    if( connection != null ){
      try{
        if( connection.isClosed ){
          createConnection()
        }else{
          datasource += connection
        }
      }catch{
        case e:Exception => {
          e.printStackTrace
          println( "close database exception" )
          throw new Exception
        }
      }
    }
  }

  private[this] def clearConnection(): Unit = synchronized{
    try{
      println( "jdbc release ..." )
      if( datasource.length > 0 ){
        ( 0 until datasource.length ).foreach{
          i =>{
            if( !datasource(i).isClosed ){
              datasource(i).close
            }
          }
        }
        datasource.clear
      }
    }catch{
      case e:Exception => println( s"failed release jdbc....${e}" )
    }
  }


  private[this] def registerShutdownHook(): Unit ={
    Runtime.getRuntime.addShutdownHook( new Thread(){
      override def run(): Unit = {
        clearConnection
      }
    } )
  }


  def executeUpdate( sql:String , params:Array[Any] ) : Int = {
    var rtn = 0
    var conn = None:Option[Connection]
    var pstmt = None:Option[PreparedStatement]

    try {
      conn = Some(getConnection())
      pstmt = Some(conn.get.prepareStatement(sql))
      (0 until params.length).foreach(
        i => {
          pstmt.get.setObject(i + 1, params(i))
          None
        })
      rtn = pstmt.get.executeUpdate()
    }catch {
      case e:Exception => {
        e.printStackTrace
        throw new Exception
      }
    } finally {
      if ( !conn.isEmpty && !conn.get.isClosed ) {
        datasource += conn.get
      }
    }
    rtn
  }

  def executeQuery( sql:String , params:Array[Any]=Array() ): Option[ResultSet] ={
    var connection = None:Option[Connection]
    var pstmt = None:Option[PreparedStatement]
    var rs = None:Option[ResultSet]
    try{
      connection = Some( getConnection() )
      pstmt = Some( connection.get.prepareStatement( sql ) )

      ( 0 until params.length ).foreach(
        i =>{
          pstmt.get.setObject( i+1 , params(i) )
        }
      )
      rs = Some( pstmt.get.executeQuery )

      rs
    }catch{
      case e:Exception => {
        e.printStackTrace
        throw new Exception
      }
    }finally {
      if( !connection.isEmpty && !connection.get.isClosed ){
        datasource += connection.get
      }
    }
  }


  def main(args: Array[String]): Unit = {

    // dbuscard,dguid,starttime,endtime,conTime,alarmtype,dealuid,uid,deal,speed,wspeed,alarmLevel,updateTime,startpos,endpos

      //      //        println(rs.getString(1))


//      val sql =
//        """
//          |insert into T_AlarmJ(dbuscard,dguid,starttime,endtime)
//          |values(?,?,?,?)
//        """.stripMargin
//      executeUpdate(sql,Array("苏L26068","64683461771","2019-07-09 16:24:09.0","2019-07-09 16:24:09.0"))
//    createConnection()
//    val sql2 =
//      """
//        |DELETE  FROM T_AlarmJ
//        |WHERE startpos is null
//      """.stripMargin
//    executeUpdate(sql2,Array())

    val res = executeQuery("Select * FROM T_AlarmJ where alarmtype=2").get
//    val res = executeQuery("SELECT Name FROM SysColumns WHERE id=Object_Id('T_AlarmJ')").get
    //

//    while (res.next()) {
//      println(res.getString(1))
//    }
    while (res.next()) {
      println(res.getString(1), res.getString(2), res.getString(3), res.getString(4),
        res.getString(5), res.getString(6), res.getString(7), res.getString(8),
        res.getString(9), res.getString(10), res.getString(11), res.getString(12),
        res.getString(13), res.getString(14), res.getString(15),res.getString(16),
        res.getString(17),res.getString(18),res.getString(19),res.getString(20))
    }

  }
}