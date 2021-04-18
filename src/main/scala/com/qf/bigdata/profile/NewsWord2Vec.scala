package com.qf.bigdata.profile

import com.qf.bigdata.profile.conf.Config
import com.qf.bigdata.profile.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

/**
  * 训练文章词向量：输入：dwb_news.article_top_terms。输出：dwb_news.article_top_terms_w2v
  */
object NewsWord2Vec {
  private val logger = LoggerFactory.getLogger(NewsWord2Vec.getClass);

  def main(args: Array[String]): Unit = {
    //1. 解析参数
    Logger.getLogger("org").setLevel(Level.WARN)
    val params: Config = Config.parseConfig(NewsWord2Vec, args)
    System.setProperty("HADOOP_USER_NAME", params.proxyUser)
    logger.warn("job is running , please wait ...")
    //2. 获取sparksession
    val ss: SparkSession = SparkUtils.getSparkSession(params.env, "NewsWord2Vec")
    import ss.implicits._

    //3. 如果是本地模式调试的话就限制一下数据打印的条数
    var limitData = ""
    if (params.env.trim.equalsIgnoreCase("dev")) {
      limitData = "limit 10"
    }

    //4. dwb_news.article_top_terms中数据
    val sourceTermsDataSql =
      s"""
         |select
         |article_id,
         |terms,
         |topterm
         |from
         |dwb_news.artile_top_terms
         |where
         |terms is not null
         |and
         |topterm is not null
         |$limitData
         |""".stripMargin

    //4.1 原始数据
    val sourceDF: DataFrame = ss.sql(sourceTermsDataSql)

    //4.2 将content_terms转换为一个数组
    val documentDF: DataFrame = sourceDF.map(elem => {
      //4.2.1 获取到词组并切割封装到数组
      val terms: Array[String] = elem.getAs("terms").toString.split(",")
      val top_terms: Array[String] = elem.getAs("topterm").toString.split(",")
      val article_id: String = elem.getAs("article_id").toString
      (article_id, terms, top_terms)
    }).toDF("article_id", "terms", "top_terms")

    //5. word2vec模型
    val word2VecModel = new Word2Vec() // 词向量空间
      .setInputCol("terms") // 将文章所有的分词传入到模型
      .setOutputCol("vector") // 输出列名
      .setVectorSize(64) // 向量的维度，说白了就是把词映射到多少维的空间上。生产环境大语料库：128、258
      .setMinCount(5) // 只有超过指定的词频这个词才会参与训练
      .setNumPartitions(2) // 分区越多，速度越快，但是精度就越低
      .setMaxIter(2) // 设置迭代次数，迭代次数越多，精度就越高，但是速度就越慢。一般这个迭代的次数都会小于等于分区的次数
      .setWindowSize(8) // 设置这个词的上下文信息（这个词的周围有8个词作为它的上下文）
      .fit(documentDF)

    /*
     * word   verctor
     * 朱登瑞  [1.2123, 2.12312, 12312.12, 1222, ...]
     */
    //6. 获取向量空间的值封装到df
    val w2v: DataFrame = word2VecModel.getVectors
    //7. 创建spark sql的udf函数
    //7.1 将vector转换为array
    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrayUDF = udf(toArr) // 定义udf函数
    w2v.withColumn("vector", toArrayUDF('vector)).createTempView("word_vec")

    //8. 将原始数据转换为列，方便日后和词向量进行关联
    documentDF.withColumn("top_terms", explode($"top_terms")).createTempView("source_term")

    //9. 将source_term和word_vec关联
    val termVecSql =
      """
        |with t1 as (
        |select article_id, top_terms from source_term
        |),
        |t2 as (
        |select word, vector from word_vec
        |)
        |select article_id, top_terms, vector from t1 left join t2 on t1.top_terms = t2.word
        |""".stripMargin

    /*
     * article_id   top_terms   vector
     * 12312        朱登瑞       [1.1111,22.2222,....]
     */

    val keyWordVec: DataFrame = ss.sql(termVecSql)

    //10. 写出到hive
    keyWordVec.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_top_terms_w2v")

    //11. 释放资源
    ss.stop()
    logger.warn("job finish success")
  }
}