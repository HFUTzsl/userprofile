package com.qf.bigdata.profile.utils

import java.util

import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.qf.bigdata.profile.conf.Config
import com.qf.bigdata.profile.utils.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.collection.{JavaConversions, mutable}

/**
  * @Author shanlin
  * @Date Created in  2021/1/18 14:08
  *
  */
object NewsContentSegment {
  private val logger = LoggerFactory.getLogger(NewsContentSegment.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val params = Config.parseConfig(NewsContentSegment, args)
    System.setProperty("HADOOP_USER_NAME", params.proxyUser)
    logger.warn("job is running,plz wait ... ")

    val ss = SparkUtils.getSparkSession(params.env, "NewsContentSegment")
    import ss.implicits._
    var limitData = ""

    if (params.env.trim.equalsIgnoreCase("dev")) {
      limitData = "limit 10"
    }

    val sourceArticleDataSql =
      s"""
         |select
         |article_id,
         |content
         |from
         |dwb_news.news_article_zh $limitData
         |""".stripMargin

    val sourceDF: DataFrame = ss.sql(sourceArticleDataSql)
    sourceDF.show()
    val termDF: DataFrame = sourceDF.mapPartitions(partition => {
      var resTermList: List[(String, String)] = List[(String, String)]()
      partition.foreach(row => {
        //5. 对conteng进行分词
        val articleId: String = row.getAs("article_id").toString
        val content: String = row.getAs("content").toString
        val termList: util.List[Term] = StandardTokenizer.segment(content)
        // 6. 去除停用词
        val termsListNoStop: util.List[Term] = CoreStopWordDictionary.apply(termList)
        val termsListNoStopAsScala: mutable.Buffer[Term] = JavaConversions.asScalaBuffer(termsListNoStop)
        // 7. 字符串中保留了所有的名词以及去除了单个汉字并且单词之间使用逗号隔开
        val contentTerm: String = termsListNoStopAsScala.filter(term => {
          term.nature.startsWith("n") && term.word.length != 1 // 保留名词(词性是以n开头)，
        }).map(term => term.word).mkString(",") // 只去除词，不要词性。
        val res = (articleId, contentTerm)
        // 8. 去除空值
        if (contentTerm.length != 0) {
          resTermList = res :: resTermList
        }
      })
      resTermList.iterator
    }).toDF("article_id", "content_terms")
    termDF.show
    termDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_terms")

    ss.stop()
    logger.warn("job finish success")
  }
}
