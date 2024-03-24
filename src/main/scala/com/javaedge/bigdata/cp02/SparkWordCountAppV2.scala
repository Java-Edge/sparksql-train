package com.javaedge.bigdata.cp02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 配置文件启动
 */
object SparkWordCountAppV2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile(args(0))

    /**
     * 结果按照单词的出现的个数的降序排列
     */
    rdd.flatMap(_.split(",")).map(word => (word, 1))
      .reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false)
      .map(x => (x._2, x._1))
      .saveAsTextFile(args(1))
    //.collect().foreach(println)

    //.sortByKey().collect().foreach(println)
    //.saveAsTextFile("/sparksql-train/out")
    //.collect().foreach(println)

    //rdd.collect().foreach(println)

    sc.stop()
  }
}
