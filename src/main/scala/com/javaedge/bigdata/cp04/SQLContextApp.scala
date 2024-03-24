package com.javaedge.bigdata.cp04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * 了解即可，已过时
 */
object SQLContextApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SQLContextApp").setMaster("local")
    // 此处一定要把SparkConf传进来
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"

    val df: DataFrame = sqlContext.read.text(projectRootPath + "/data/input.txt")
    df.show()

    sc.stop()
  }

}
