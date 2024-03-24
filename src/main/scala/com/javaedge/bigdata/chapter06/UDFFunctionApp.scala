package com.javaedge.bigdata.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 统计每个人爱好的个数
 * pk：3
 * jepson： 2
 *
 *
 * 1）定义函数
 * 2）注册函数
 * 3）使用函数
 */
object UDFFunctionApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("HiveSourceApp")
      .getOrCreate()


    import spark.implicits._

    val infoRDD: RDD[String] = spark.sparkContext.textFile(
      "/Users/javaedge/Downloads/soft/sparksql-train/data/hobbies.txt")
    val infoDF: DataFrame = infoRDD.map(_.split("###")).map(x => {
      Hobbies(x(0), x(1))
    }).toDF

    infoDF.show(false)

    // TODO... 定义函数 和 注册函数
    spark.udf.register("hobby_num", (s: String) => s.split(",").size)

    infoDF.createOrReplaceTempView("hobbies")

    //TODO... 函数的使用
    spark.sql("select name, hobbies, hobby_num(hobbies) as hobby_count from hobbies").show(false)

    // select name, hobby_num(hobbies) from xxx

    spark.stop()
  }

  private case class Hobbies(name: String, hobbies: String)
}
