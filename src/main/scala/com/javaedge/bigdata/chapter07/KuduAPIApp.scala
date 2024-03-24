package com.javaedge.bigdata.chapter07

import java.util

import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client._

object KuduAPIApp {



  def main(args: Array[String]): Unit = {
    val KUDU_MASTERS = "localhost"
    val client: KuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build()
    val tableName = "ods"

//     createTable(client, tableName)

  //  insertRows(client, tableName)

//    deleteTable(client, "ods")
//    deleteTable(client, "province_city_stat")
    deleteTable(client, "ods_20181007")
    deleteTable(client, "province_city_stat_20181007")
    deleteTable(client, "app_stat_20181007")
    deleteTable(client, "area_stat_20181007")
//    deleteTable(client, "region_stat")
//    deleteTable(client, "app_stat")
//    createTable(client, tableName)

//    query(client, tableName)
//    println("======")
//    alterRow(client, tableName)
//    println(".........")
//    query(client, tableName)

//    val newTableName = "pk_kudu"
//    renameTable(client, tableName, newTableName)

    client.close()
  }


  /**
    * 修改表名
    */
  def renameTable(client: KuduClient, tableName: String, newTableName: String) = {

    val options: AlterTableOptions = new AlterTableOptions()
    options.renameTable(newTableName)
    client.alterTable(tableName, options)
  }
  /**
    * 修改表数据
    */
  def alterRow(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)
    val session: KuduSession = client.newSession()

    val update: Update = table.newUpdate()
    val row: PartialRow = update.getRow
    row.addString("word", "pk-10")
    row.addInt("cnt", 8888)
    session.apply(update)
  }

  /**
    * 查询数据
    */
  def query(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)

    val scanner: KuduScanner = client.newScannerBuilder(table).build()

    while(scanner.hasMoreRows) {
      val iterator: RowResultIterator = scanner.nextRows()

      while(iterator.hasNext) {
        val result: RowResult = iterator.next()
        println(result.getString("word") + " => " + result.getInt("cnt"))
      }
    }

  }


  /**
    * 删除表
    */
  def deleteTable(client: KuduClient, tableName: String) = {
    client.deleteTable(tableName)
  }

  /**
    * 插入数据
    *
    * 作业：自己找资料进行批量插入
    */
  def insertRows(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)  // 根据表名获取kudu的表
    val session: KuduSession = client.newSession() // JPA Hibernate

    for(i<-1 to 10) {
      val insert: Insert = table.newInsert()
      val row: PartialRow = insert.getRow
      row.addString("word",s"pk-$i")
      row.addInt("cnt", 100+i)

      session.apply(insert)
    }

  }



  /**
    * 创建表
    */
  def createTable(client: KuduClient, tableName: String): Unit = {
    import scala.collection.JavaConverters._
    val columns = List(
      new ColumnSchema.ColumnSchemaBuilder("word", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("cnt", Type.INT32).build()
    ).asJava

    val schema = new Schema(columns)

    val options: CreateTableOptions = new CreateTableOptions()
    options.setNumReplicas(1)

    val parcols: util.LinkedList[String] = new util.LinkedList[String]()
    parcols.add("word")
    options.addHashPartitions(parcols,3)

    client.createTable(tableName,schema,options)
  }

}
