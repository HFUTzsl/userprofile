package com.qf.bigdata.profile.utils.test

import com.qf.bigdata.profile.utils.SparkUtils
import org.apache.spark.sql.DataFrame

/**
  * @Author shanlin
  * @Date Created in  2021/4/13 13:44
  *
  */
object Pg1 {
  val spark = SparkUtils.getSparkSession("dev", "test")

  def getPg1(): DataFrame = {
    val df2: DataFrame = spark.sql(
      """
select * from  dws_news.zsl
        """.stripMargin)
    df2
  }
}
