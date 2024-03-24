package com.javaedge.bigdata.cp04

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"
    // DF/DS编程的入口点
    val spark: SparkSession = SparkSession.builder()
      .master("local").getOrCreate()

    // 读取文件的API
    val df: DataFrame = spark.read.text(projectRootPath + "/data/input.txt")

    // 业务逻辑处理，通过DF/DS提供的API完成业务
    df.printSchema()
    // 展示出来  只有一个字段，string类型的value
    df.show()

    spark.stop()
  }
}
