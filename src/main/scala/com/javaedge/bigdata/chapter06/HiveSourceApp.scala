package com.javaedge.bigdata.chapter06

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveSourceApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("HiveSourceApp")
      // Spark访问Hive时，要开启Hive支持
      .enableHiveSupport()
      .getOrCreate()


    // 走的就是连接 default数据库中的表
//    spark.sql("default.my_table").show()
//    spark.sql("CREATE TABLE my_table (id INT,name STRING)").show()
    spark.sql("INSERT INTO my_table VALUES (1, 'John')").show()
    spark.sql("select * from my_table").show()


    // input(Hive/MySQL/JSON...) ==> 处理 ==> output (Hive)


    import spark.implicits._

    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table")
    val sinkTable = config.getString("db.default.sink.table")

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val jdbcDF: DataFrame = spark.read
      .jdbc(url, s"$database.$table", connectionProperties).filter($"cnt" > 100)

    //jdbcDF.show()

    jdbcDF.write.saveAsTable("browser_stat_hive")

    // TODO...  saveAsTable和insertInto的区别
    jdbcDF.write.insertInto("browser_stat_hive_1")

    spark.stop()

  }
}
