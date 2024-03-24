package com.javaedge.bigdata.chapter06

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object JDBCClientApp {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn: Connection = DriverManager.getConnection("jdbc:hive2://localhost:10000")
    val pstmt: PreparedStatement = conn.prepareStatement("show tables")

    val rs: ResultSet = pstmt.executeQuery()

    while (rs.next()) {
      println(rs.getObject(1) + " : " + rs.getObject(2))
    }
  }
}
