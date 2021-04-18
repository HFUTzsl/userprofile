package com.qf.bigdata.profile.utils.test

import com.qf.bigdata.profile.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author shanlin
  * @Date Created in  2021/4/13 13:46
  *
  */
object Pg2 {
  def getPg2(): DataFrame = {
  val spark: SparkSession = SparkUtils.getSparkSession("dev","getpg2")
    val df1: DataFrame = spark.sql(
      """
select * from dws_news.zsl
    """.stripMargin)
    df1
  }
}
