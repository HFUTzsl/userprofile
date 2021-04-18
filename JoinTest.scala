package com.qf.bigdata.profile.utils.test

import org.apache.spark.sql.DataFrame

/**
  * @Author shanlin
  * @Date Created in  2021/4/13 13:49
  *
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val df1: DataFrame = Pg1.getPg1()
    val df2: DataFrame = Pg2.getPg2()
    df1.join(df2, df1("name") === df2("name")).show()
  }
}
