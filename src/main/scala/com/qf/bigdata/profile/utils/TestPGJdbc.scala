package com.qf.bigdata.profile.utils

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @Author shanlin
  * @Date Created in  2021/4/2 16:31
  *
  */
object TestPGJdbc {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val df1: DataFrame = getPGDF("test", "zsl.xinxi")
    df1.show()
    println("==========分隔线============")
    val df2: DataFrame = getPGDF("test", "zsl.info")
    df2.show()

    val df3: DataFrame = df1.join(df2, df1("name") === df2("姓名"))
    //    df3.show()
    //    df3.select("name","体重").filter(df3("体重")>100).show()
    //    df3.show()
    val df4: DataFrame = df3.withColumn("newCol", df3("age") + df3("体重"))
    df4.filter(df4("newCol") > 120).show()


  }

  def getPGDF(database: String, tableName: String): DataFrame = {
    val jdbc_conf: Map[String, String] = Map(
      "url" -> s"jdbc:postgresql://localhost:5432/${database}", //设置postgresql的链接地址和指定数据库
      "driver" -> "org.postgresql.Driver", //设置postgresql的链接驱动
      "dbtable" -> s"${tableName}", //获取数据所在表的名成
      "user" -> "postgres", //连接postgresql的用户
      "password" -> "88888888" //连接用户的密码
    )
    val session = getSparkSession()
    val dataframe: DataFrame = session.read.format("jdbc") //设置读取方式
      .options(jdbc_conf) //放入jdbc的配置信息
      .load()
    dataframe
  }

  def getSparkSession(): SparkSession = {
    val spark = new sql.SparkSession
    .Builder()
      .appName("source_data_mysql001")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()
    spark
  }
}
