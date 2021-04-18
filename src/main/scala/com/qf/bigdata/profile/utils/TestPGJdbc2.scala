package com.qf.bigdata.profile.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame

/**
  * @Author shanlin
  * @Date Created in  2021/4/2 18:10
  *
  */
object TestPGJdbc2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = new sql.SparkSession
    .Builder()
      .appName("source_data_mysql001")
      .master("local[*]")
      .getOrCreate()
    val jdbc_conf1: Map[String, String] = Map(
      "url" -> "jdbc:postgresql://localhost:5432/test", //设置mysql的链接地址和指定数据库
      "driver" -> "org.postgresql.Driver", //设置MySQL的链接驱动
      "dbtable" -> "zsl.xinxi", //获取数据所在表的名成
      "user" -> "postgres", //连接mysql的用户
      "password" -> "88888888" //连接用户的密码
    )
    val data_mysql1: DataFrame = spark.read.format("jdbc") //设置读取方式
      .options(jdbc_conf1) //放入jdbc的配置信息
      .load()

    //    data_mysql1.show()


    val jdbc_conf2: Map[String, String] = Map(
      "url" -> "jdbc:postgresql://localhost:5432/test", //设置mysql的链接地址和指定数据库
      "driver" -> "org.postgresql.Driver", //设置MySQL的链接驱动
      "dbtable" -> "zsl.info", //获取数据所在表的名成
      "user" -> "postgres", //连接mysql的用户
      "password" -> "88888888" //连接用户的密码
    )
    val data_mysql2: DataFrame = spark.read.format("jdbc") //设置读取方式
      .options(jdbc_conf2) //放入jdbc的配置信息
      .load()

    data_mysql1.join(data_mysql2, data_mysql1("name") === data_mysql2("姓名")).show()
    //    data_mysql2.show()
  }
}
