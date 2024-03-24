package com.javaedge.bigdata.chapter05

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DataSourceApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local").getOrCreate()

    // text(spark)
    // json(spark)
    // common(spark)
//     parquet(spark)
    // convert(spark)

//     jdbc1(spark)
//     jdbc2(spark)
    jdbcConfig(spark)
    spark.stop()
  }

  // TODO 代码打包，提交到YARN或者Standalone集群上去，体会driver参数意义
  private def jdbcConfig(spark: SparkSession): Unit = {
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

    val jdbcDF: DataFrame = spark.read.jdbc(url, s"$database.$table", connectionProperties)

    jdbcDF.filter($"id" > 158)
    .write.jdbc(url, s"$database.$sinkTable", connectionProperties)
  }

  def jdbc1(spark: SparkSession): Unit = {
    import spark.implicits._

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "education.user")
      .option("user", "root")
      .option("password", "123456")
      .load()

    jdbcDF.filter($"id" > 300).show(100)
  }

  def jdbc2(spark: SparkSession): Unit = {
    import spark.implicits._

    val url = "jdbc:mysql://localhost:3306"
    val srcTable = "education.user"

    val connProps = new Properties()
    connProps.put("user", "root")
    connProps.put("password", "123456")

    val jdbcDF: DataFrame = spark.read.jdbc(url, srcTable, connProps)

    // 若目标表不存在，会自动帮你创建
    jdbcDF.filter($"id" > 300)
      .write.jdbc(url, "education.user_bak", connProps)
  }

  // 存储类型转换：JSON==>Parquet
  private def convert(spark: SparkSession): Unit = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.format("json")
      .load("/Users/javaedge/Downloads/soft/sparksql-train/data/people.json")
    jsonDF.show()

    jsonDF.filter("age>20")
      .write.format("parquet").mode(SaveMode.Overwrite).save("out")

    spark.read.parquet(
      "/Users/javaedge/Downloads/soft/sparksql-train/out").show()

  }

  // Parquet数据源
  private def parquet(spark: SparkSession): Unit = {
    import spark.implicits._

    val parquetDF: DataFrame = spark.read.parquet("/Users/javaedge/Downloads/soft/sparksql-train/data/users.parquet")
    parquetDF.printSchema()
    parquetDF.show()

    parquetDF.select("name", "favorite_numbers")
      .write.mode("overwrite")
      .option("compression", "none")
      .parquet("out")

    //    spark.read.parquet("/Users/javaedge/Downloads/soft/sparksql-train/out").show()
  }

  // 标准API写法
  private def common(spark: SparkSession): Unit = {
    import spark.implicits._

    val textDF: DataFrame = spark.read.format("text").load(
      "/Users/javaedge/Downloads/soft/sparksql-train/data/people.txt")
    val jsonDF: DataFrame = spark.read.format("json").load(
      "/Users/javaedge/Downloads/soft/sparksql-train/data/people.json")
    textDF.show()
    println("~~~~~~~~")
    jsonDF.show()

    jsonDF.write.format("json").mode("overwrite").save("out")

  }

  // JSON
  def json(spark: SparkSession): Unit = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.json(
      "/Users/javaedge/Downloads/soft/sparksql-train/data/people.json")
    //    jsonDF.show()

    // 简单 JSON：只要age>20的数据
    jsonDF.filter("age > 20")
      .select("name")
      .write.mode(SaveMode.Overwrite).json("out")

    // 嵌套 JSON
    val jsonDF2: DataFrame = spark.read.json(
      "/Users/javaedge/Downloads/soft/sparksql-train/data/people2.json")
    jsonDF2.show()

    jsonDF2.select($"name",
      $"age",
      $"info.work".as("work"),
      $"info.home".as("home"))
      .write.mode("overwrite")
      .json("out")
  }

  // 文本
  def text(spark: SparkSession): Unit = {
    import spark.implicits._

    val textDF: DataFrame = spark.read.text(
      "/Users/javaedge/Downloads/soft/sparksql-train/data/people.txt")

    val result: Dataset[String] = textDF.map(x => {
      val splits: Array[String] = x.getString(0).split(",")
      splits(0).trim
    })
    //    result.write.mode("overwrite").text("out")
    result.write.mode(SaveMode.Overwrite).text("out")
  }
}
