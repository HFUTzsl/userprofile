package com.qf.bigdata.profile


import com.qf.bigdata.profile.conf.Config
import com.qf.bigdata.profile.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, IDFModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 通过TF_IDF算法提取关键字：article-terms中
  */
object NewsKeyWord {

  private val logger = LoggerFactory.getLogger(NewsKeyWord.getClass);


  def main(args: Array[String]): Unit = {
    //1. 解析参数
    Logger.getLogger("org").setLevel(Level.WARN)
    val params: Config = Config.parseConfig(NewsKeyWord, args)
    System.setProperty("HADOOP_USER_NAME", params.proxyUser)
    logger.warn("job is running , please wait ...")

    //2. 获取提取关键词的个数
    val topk: Int = params.topK

    //2. 获取sparksession
    val ss: SparkSession = SparkUtils.getSparkSession(params.env, "NewsKeyWord")
    import ss.implicits._

    //3. 如果是本地模式调试的话就限制一下数据打印的条数
    var limitData = ""
    if (params.env.trim.equalsIgnoreCase("dev")) {
      limitData = "limit 10"
    }

    //4. dwb_news.article_terms中数据
    val sourceTermsDataSql =
      s"""
         |select
         |article_id,
         |content_terms
         |from
         |dwb_news.article_terms
         |where
         |content_terms is not null $limitData
         |""".stripMargin

    //4.1 原始数据
    val sourceDF: DataFrame = ss.sql(sourceTermsDataSql)

    //4.2 将content_terms转换为一个数组
    val documentDF: DataFrame = sourceDF.map(elem => {
      //4.2.1 获取到词组并切割封装到数组
      val terms: Array[String] = elem.getAs("content_terms").toString.split(",")
      val article_id: String = elem.getAs("article_id").toString
      (article_id, terms)
    }).toDF("article_id", "terms")

    //5. 创建一个TF:TF模型
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("terms") // 输入列
      .setOutputCol("rawFeature") // 输出列
      .setVocabSize(512 * 512) // 词汇表的大小
      .setMinDF(1) // 这个词语只要出现在一个文档中我就记录
      .fit(documentDF)

    /*
     * (461, [93, 8, 12, 232], [1.0, 1.0, 0.0, 1.0])
     *
     * 461 : 词汇表大小
     * 第一个数组：每个词在词汇表中的索引值
     * 第二个数组：每个词出现的词频
     */
    val featureData: DataFrame = cvModel.transform(documentDF)

    //6. IDF模型
    val idf: IDF = new IDF().setInputCol("rawFeature").setOutputCol("feature")
    val idfModel: IDFModel = idf.fit(featureData)
    //6.1 TF-IDF
    val rescalaData: DataFrame = idfModel.transform(featureData)

    //7. 获取词汇表数组
    val vocabulary: Array[String] = cvModel.vocabulary
    //8. 获取到关键词
    val topTermDF: DataFrame = rescalaData.select("article_id", "terms", "feature")
      .map(elem => {
        // 使用map的原因是想要将生成TF-IDF按照向量值大小排序
        val terms: Seq[String] = elem.getAs[Seq[String]]("terms")
        val article_id: String = elem.getAs("article_id").toString
        val features: SparseVector = elem.getAs[SparseVector]("feature")
        val topTerms: String = features.indices.zip(features.values).sortBy(-_._2).take(topk).map(x => vocabulary(x._1)).mkString(",")
        (article_id, topTerms, terms.mkString(","))
      }).toDF("article_id", "topTerm", "terms")
    //9. 写会hive
    topTermDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.artile_top_terms")
    //10. 释放资源
    ss.stop()
    logger.warn("job finish success")
  }
}