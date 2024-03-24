package com.javaedge.bigdata.cp04

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetApp {

  def main(args: Array[String]): Unit = {
    val projectRootPath = "/Users/javaedge/Downloads/soft/sparksql-train"
    val spark = SparkSession.builder()
      .master("local").appName("DatasetApp")
      .getOrCreate()
    import spark.implicits._

    // 创建一个包含一条记录的Seq，这条记录包含一个名为 "JavaEdge" 年龄为 18 的人员信息
    val ds: Dataset[Person] = Seq(Person("JavaEdge", "18"))
      // 将Seq转换为一个Dataset[Person]类型数据集，该数据集只包含一条记录
      .toDS()
    ds.show()

    val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
    primitiveDS.map(x => x + 1).collect().foreach(println)

    val peopleDF: DataFrame = spark.read.json(projectRootPath + "/data/people.json")
    val peopleDS: Dataset[Person] = peopleDF.as[Person]
    peopleDS.show(false)
    // 弱语言类型，运行时才会报错
//    peopleDF.select("nameEdge").show()
    peopleDF.select("name").show()
    // 编译期报错
//    peopleDS.map(x => x.nameEdge).show()
    peopleDS.map(x => x.name).show()

    spark.stop()
  }

  /**
   * 自定义的 case class，其中包含两个属性
   */
  private case class Person(name: String, age: String)

}
