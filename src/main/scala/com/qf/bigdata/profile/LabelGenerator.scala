package com.qf.bigdata.profile


import com.qf.bigdata.profile.conf.Config
import com.qf.bigdata.profile.utils.{SparkUtils, TableUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory


/**
  * 生成基础的特征标签，然后最终存入到clickhouse
  */
object LabelGenerator {

  private val logger = LoggerFactory.getLogger(LabelGenerator.getClass);

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN) // 将日志的级别调整减少不必要的日志显示在控制台
    //1. 解析参数
    val params: Config = Config.parseConfig(LabelGenerator, args)
    logger.info("job is running, please wait for a moment")

    //2. 获取到sparkSession
    val ss: SparkSession = SparkUtils.getSparkSession(params.env, LabelGenerator.getClass.getSimpleName)

    //2.1 设置spark操作的参数:读取hudi和hdfs的时候采取指定的过滤器来消除读取表的时候的无用的信息
    ss.sparkContext.hadoopConfiguration
      .setClass("mapreduce.input.pathFilter.class",
        classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter],
        classOf[org.apache.hadoop.fs.PathFilter])
    import ss.implicits._
    /*
     * user_ro和event_ro
     * 生成表
     * uid  gender  age   region      email_shufflx  model
     * 1    男       32   15549497595 qq.com         huawei mate 20 pro
     */

    val baseFeatureSql =
      """
        |select
        |t1.distinct_id as uid,
        |t1.gender,
        |t1.age,
        |case when length(t1.mobile) >= 11 then substring(t1.mobile, -11, length(t1.mobile)) else '' end as region,
        |case when size(split(t1.email, '@')) = 2 then split(t1.email, '@')[1] else '' end as email_shuffix,
        |t2.network_type as model
        |from
        |ods_news.user_ro as t1
        |left join
        |dwb_news.user_base_info as t2
        |on
        |t1.distinct_id = t2.uid
        |""".stripMargin

    // 3 建表
    ss.sql(
      """
        |create table if not exists dws_news.user_profile_base(
        |uid string,
        |gender string,
        |age string,
        |region string,
        |email_shuffix string,
        |model string
        |)
        |stored as parquet
        |""".stripMargin)

    // 3.1 查询生成的dataframe
    val baseFeatureDF: DataFrame = ss.sql(baseFeatureSql)

    //3.2 缓存：1是存在hdfs，2是存在clickhouse
    baseFeatureDF.cache();
    //4. 写在hdfs中
    baseFeatureDF.write.mode(SaveMode.Overwrite).saveAsTable("dws_news.user_profile_base")

    //5. 向clickhouse中写: meta._1:列名， meta._2:sql的占位符
    val meta = TableUtils.getClickHouseUserProfileBaseTable(baseFeatureDF, params)

    //6. 插入数据
    //6.1 sql语句
    val insertSql =
    s"""
       |insert into ${TableUtils.USER_PROFILE_CLICKHOUSE_TABLE} (${meta._1}) values(${meta._2})
       |""".stripMargin
    logger.warn(s"${insertSql}")
    //6.2 执行插入数据
    baseFeatureDF.foreachPartition(partition => {
      TableUtils.insertBaseFeatureTable(partition, insertSql, params)
    })
    //6.3
    baseFeatureDF.unpersist()
    ss.stop()
    logger.warn("job has success")
  }
}