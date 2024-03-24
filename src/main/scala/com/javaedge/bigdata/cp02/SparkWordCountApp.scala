package com.javaedge.bigdata.cp02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 词频统计
 *
 * 输入：文件
 * 需求：统计出文件中每个单词出现的次数
 * 1）读每一行数据
 */
object SparkWordCountApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkWordCountApp")
    val sc = new SparkContext(sparkConf)
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"
    val rdd = sc.textFile(projectRootPath + "/data/input.txt")

    // 2）按照分隔符把每一行的数据拆成单词
    rdd.flatMap(_.split(","))
      // 3）每个单词赋上次数为1
      .map(word => (word, 1))
      // 4）按照单词进行分发，然后统计单词出现的次数
      .reduceByKey(_ + _)
      // 结果按单词频率降序排列,既然之前是 <单词，频率> 且 sortKey 只能按 key 排序，那就在这里反转 kv 顺序
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .collect().foreach(println)
    sc.stop()


    rdd.flatMap(_.split(","))
      // 3）每个单词赋上次数为1
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(projectRootPath + "/data/out/output.txt")
    sc.stop()


    // 2）按照分隔符把每一行的数据拆成单词
    rdd.flatMap(_.split(","))
      // 3）每个单词赋上次数为1
      .map(word => (word, 1))
      // 4）按照单词进行分发，然后统计单词出现的次数
      .reduceByKey(_ + _)
      // 结果按单词频率降序排列,既然之前是 <单词，频率> 且 sortKey 只能按 key 排序，那就在这里反转 kv 顺序
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .collect().foreach(println)
    // 5）把结果输出到文件中
    //      .saveAsTextFile("/Users/javaedge/Downloads/soft/sparksql-train/data/output.txt")
    sc.stop()
  }
}