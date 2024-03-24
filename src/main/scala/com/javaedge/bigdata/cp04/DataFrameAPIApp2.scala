package com.javaedge.bigdata.cp04

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameAPIApp2 {

  def main(args: Array[String]): Unit = {
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"
    val spark = SparkSession.builder()
      .master("local").appName("DataFrameAPIApp")
      .getOrCreate()

    val zips: DataFrame = spark.read.json(projectRootPath + "/data/zips.json")
    // 查看schema信息
    zips.printSchema()

    /**
     * loc的信息没展示全，超过一定长度就使用...来展示
     * 默认只显示前20条：
     * show() ==> show(20) ==> show(numRows, truncate = true)
     */
    zips.show(5)
    zips.show(5, truncate = false)

    // 先从数据集 zips 中获取前三行数据（或者说前三个元素）
    zips.head(3)
      // 使用 foreach(println) 将这三行数据打印输出到控制台
      .foreach(println)
    // 获取数据集 zips 中的第一个元素（或者说第一行数据）
    zips.first()
    // 获取数据集 zips 中的前五个元素（或者说前五行数据）
    zips.take(5)

    val count: Long = zips.count()
    println(s"Total Counts: $count")

    // 过滤出大于40000，字段重新命名
    zips.filter(zips.col("pop") > 40000)
      .withColumnRenamed("_id", "new_id")
      .show(5, truncate = false)


    import org.apache.spark.sql.functions._
    // 统计加州pop最多的10个城市名称和ID  desc是一个内置函数
    zips.select("_id", "city", "pop", "state")
      .filter(zips.col("state") === "CA")
      .orderBy(desc("pop"))
      .show(5, truncate = false)

    zips.createOrReplaceTempView("zips")
    spark.sql("select _id,city,pop,state" +
      "from zips where state='CA'" +
      "order by pop desc" +
      "limit 10").show()

    spark.stop()
  }
}
