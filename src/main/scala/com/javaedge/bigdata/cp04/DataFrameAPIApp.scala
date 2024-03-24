package com.javaedge.bigdata.cp04

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameAPIApp {

  def main(args: Array[String]): Unit = {
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"
    val spark = SparkSession.builder()
      .master("local").appName("DataFrameAPIApp")
      .getOrCreate()
    import spark.implicits._


    val people: DataFrame = spark.read.json(projectRootPath + "/data/people.json")

    // 查看DF的内部结构：列名、列的数据类型、是否可以为空
    people.printSchema()

    // 展示出DF内部的数据
    people.show()

    // DF里面有两列，只要name列 ==> select name from people
    //    people.select("name").show()
    //    people.select($"name").show()

    // select * from people where age > 21
    //    people.filter($"age" > 21).show()
    //    people.filter("age > 21").show()

    // select age, count(1) from people group by age
    //    people.groupBy("age").count().show()

    // select name,age+10 from people
    // people.select($"name", ($"age"+10).as("new_age")).show()


    // 使用SQL的方式操作
    //        people.createOrReplaceTempView("people")
    //        spark.sql("select name from people where age > 21").show()
    spark.stop()
  }
}
