package com.qf.bigdata.profile.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author shanlin
  * @Date Created in  2021/4/13 11:38
  *
  */
object TestJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("dev", "test")

    val df1: DataFrame = spark.sql(
      """
select * from  dws_news.zsl
      """.stripMargin)

    println("========================")

    val df2: DataFrame = spark.sql(
      """
select * from dws_news.zslnew
      """.stripMargin)

    df1.show()
    df2.show()
    //    df1.join(df2).show()
  }
}
