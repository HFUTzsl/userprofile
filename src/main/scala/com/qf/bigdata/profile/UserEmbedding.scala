package com.qf.bigdata.profile

import com.qf.bigdata.profile.conf.Config
import com.qf.bigdata.profile.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._


/**
  * 1. ods_news.event_ro:表中包含了用户对文章的点击数据
  * 2. dwb_news.article_top_terms_w2v:此表和上表进行关联，我们就可以得出这个用户浏览果这个文章，就和文章的标签关联了
  * 3. 我们就可以对这些东西尽心处理，存放dws_news.user_content_embedding
  */
object UserEmbedding {
  private val logger = LoggerFactory.getLogger(UserEmbedding.getClass);

  def main(args: Array[String]): Unit = {
    //1. 解析参数
    Logger.getLogger("org").setLevel(Level.WARN)
    val params: Config = Config.parseConfig(UserEmbedding, args)
    System.setProperty("HADOOP_USER_NAME", params.proxyUser)
    logger.warn("job is running , please wait ...")
    //2. 获取sparksession
    val ss: SparkSession = SparkUtils.getSparkSession(params.env, "UserEmbedding")
    ss.sparkContext.hadoopConfiguration
      .setClass("mapreduce.input.pathFilter.class",
        classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter],
        classOf[org.apache.hadoop.fs.PathFilter])

    import ss.implicits._

    //3. 如果是本地模式调试的话就限制一下数据打印的条数
    var limitData = ""
    if (params.env.trim.equalsIgnoreCase("dev")) {
      limitData = "limit 10"
    }

    //4. dwb_news.article_top_terms中数据
    val userWordVecDataSql =
      s"""
         |with t1 as (
         |select
         |distinct_id as uid,
         |article_id
         |from
         |ods_news.event_ro
         |where
         |logday >= '${params.startDate}' and logday <= '${params.endDate}'
         |and
         |event = 'AppClick' and element_page = '内容详情页'
         |and
         |article_id != ''
         |and
         |article_id is not null
         |),
         |t2 as (
         |select
         |article_id,
         |vector
         |from
         |dwb_news.article_top_terms_w2v
         |where
         |vector is not null
         |)
         |select
         |t1.uid,
         |t1.article_id,
         |t2.vector
         |from
         |t1 left join t2
         |on
         |t1.article_id = t2.article_id
         |where
         |t2.vector is not null
         |$limitData
         |""".stripMargin

    //4.1 原始数据
    val sourceDF: DataFrame = ss.sql(userWordVecDataSql)

    //4.2 定义udf的自定义函数：Seq[Double] --> VectorDense
    val array2vec: UserDefinedFunction = udf((array: Seq[Double]) => Vectors.dense(array.toArray))


    //4.3 计算出用户向量
    val userEmbeddingDF: Dataset[Row] = sourceDF.withColumn("vector", array2vec($"vector")).groupBy("uid")
      .agg(Summarizer.mean($"vector").alias("user_vector")) // 该用户下的所有的词向量加和求平均-->用户向量


    //5. 写回hive表
    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrayUDF = udf(toArr) // 定义udf函数
    userEmbeddingDF.withColumn("user_vector", toArrayUDF('user_vector))
      .write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dws_news.user_content_embedding")

    //11. 释放资源
    ss.stop()
    logger.warn("job finish success")
  }
}