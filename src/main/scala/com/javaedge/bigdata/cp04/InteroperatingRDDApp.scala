package com.javaedge.bigdata.cp04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InteroperatingRDDApp {
  def main(args: Array[String]): Unit = {
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"
    val spark = SparkSession.builder()
      .master("local").appName("DatasetApp")
      .getOrCreate()
    runInferSchema(spark)

    runProgrammaticSchema(spark)
    spark.stop()
  }

  /**
   * 第二种方式：自定义编程
   */
  private def runProgrammaticSchema(spark: SparkSession): Unit = {
    import spark.implicits._
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"

    // step1
    val peopleRDD: RDD[String] = spark.sparkContext.textFile(projectRootPath + "/data/people.txt")
    // 定义一个RDD[Row]类型的变量peopleRowRDD，用于存储处理后的每行数据
    val peopleRowRDD: RDD[Row] = peopleRDD
      // 使用map方法将每行字符串按逗号分割为数组，得到一个RDD[Array[String]]
      .map(_.split(","))
      // 再次使用map方法，将数组转换为Row对象，Row对象的参数类型需要和schema中定义的一致
      // 这里假设schema中的第一个字段为String类型，第二个字段为Int类型
      .map(x => Row(x(0), x(1).trim.toInt))

    // step2
    // 描述DataFrame的schema结构
    val struct = StructType(
      // 使用StructField定义每个字段
      StructField("name", StringType, nullable = true) ::
        StructField("age", IntegerType, nullable = false) :: Nil)

    // step3
    val peopleDF: DataFrame = spark.createDataFrame(peopleRowRDD, struct)

    peopleDF.show()
  }

  /**
   * 第一种方式：反射
   * 1）定义case class
   * 2）RDD map，map中每一行数据转成case class
   */
  private def runInferSchema(spark: SparkSession): Unit = {
    import spark.implicits._
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"

    // 读取文件内容为RDD，每行内容为一个String元素
    val peopleRDD: RDD[String] = spark.sparkContext.textFile(projectRootPath + "/data/people.txt")

    // RDD转换为DataFrame的过程
    val peopleDF: DataFrame = peopleRDD
      // 1. 使用map方法将每行字符串按逗号分割为数组
      .map(_.split(","))
      // 2. 再次使用map方法，将数组转换为People对象
      .map(x => People(x(0), x(1).trim.toInt))
      // 3. 最后调用toDF将RDD转换为DataFrame
      .toDF()

    peopleDF.createOrReplaceTempView("people")
    val queryDF: DataFrame = spark.sql("select name,age" +
      "from people" +
      "where age" +
      "between 19 and 29")

    //queryDF.map(x => "Name:" + x(0)).show()  // from index
    queryDF.map(x => "Name:" + x.getAs[String]("name")).show // from field
  }

  private case class People(name: String, age: Int)
}
