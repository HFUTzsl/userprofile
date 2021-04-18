package com.qf.bigdata.profile.utils

import java.sql.PreparedStatement

import com.qf.bigdata.profile.LabelGenerator
import com.qf.bigdata.profile.conf.Config
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, NullType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}

/**
  * 生成clickhouse的数据
  */
object TableUtils {

  private val logger = LoggerFactory.getLogger(TableUtils.getClass);

  val USER_PROFILE_CLICKHOUSE_DATABASE = "app_news"
  val USER_PROFILE_CLICKHOUSE_TABLE = s"$USER_PROFILE_CLICKHOUSE_DATABASE.user_profile_base"

  /**
    * 根据baseFeatureDF中的数据生成clickhouse的表，返回的baseFeatureDF的各个列名以及占位符
    */
  def getClickHouseUserProfileBaseTable(baseFeatureDF: DataFrame, params: Config): (String, String) = {
    /*
     * 1. 将sparksql中的模型解析为下列所示的模型
     * fName: uid, gender, age, ...
     * fNameType:String, String, String,...
     * pl = ?,?,?,?
     */
    val (fName, fNameType, pl) = baseFeatureDF.schema.fields.foldLeft("", "", "")((z, f) => {
      if (z._1.nonEmpty && z._2.nonEmpty && z._3.nonEmpty) { // z不为空，后面的f应该追加到z上
        (z._1 + "," + f.name, z._2 + "," + f.name + " " + f.dataType.simpleString, z._3 + ", " + "?")
      } else {
        (f.name, f.name + " " + f.dataType.simpleString, "?") // f的值直接覆盖z
      }
    })

    //2. 将spark的表达式转换为clickhouse的表达式
    val chCol = ClickHouseUtils.dfTypeName2CH(fNameType)
    //3. 获取到连接clickhouse的cluser
    val cluster: String = params.cluster
    //4. 在clickhouse建表的sql:app_news:user_profile_base
    var chTableSql =
      s"""
         |create table $USER_PROFILE_CLICKHOUSE_TABLE($chCol)
         |ENGINE = MergeTree()
         |ORDER BY(uid)
         |""".stripMargin
    //5. 创建库
    var createDBSql = s"create database if not exists ${USER_PROFILE_CLICKHOUSE_DATABASE}"

    //6. 删除表
    var dropTableSql = s"drop table if exists ${USER_PROFILE_CLICKHOUSE_TABLE}"

    // 设计如果是连接远程的clickhouse的话
    //    if (!"".equalsIgnoreCase(cluster)) {
    //      chTableSql =
    //        s"""
    //           |create table $USER_PROFILE_CLICKHOUSE_TABLE ON CLUSTER '${cluster}' ($chCol)
    //           |ENGINE = MergeTree()
    //           |ORDER BY(uid)
    //           |""".stripMargin
    //      createDBSql = s"create database if not exists ${USER_PROFILE_CLICKHOUSE_DATABASE} ON CLUSTER '${cluster}'"
    //      dropTableSql = s"drop table if exists $USER_PROFILE_CLICKHOUSE_TABLE ON CLUSTER '${cluster}'"
    //    }
    //7. 获取到连接到clickhouse的数据源对象
    val dataSource: ClickHouseDataSource = ClickHouseUtils.getDataSource(params.username, params.password, params.url)
    val connection: ClickHouseConnection = dataSource.getConnection
    logger.warn(createDBSql)
    var ps: PreparedStatement = connection.prepareStatement(createDBSql)
    ps.execute()
    logger.warn(dropTableSql)
    ps = connection.prepareStatement(dropTableSql)
    ps.execute()
    logger.warn(chTableSql)
    ps = connection.prepareStatement(chTableSql)
    ps.execute()

    ps.close()
    connection.close()
    logger.warn("init success")
    (fName, pl)
  }

  /**
    * clickhouse根据insertSql插入数据到表
    */
  def insertBaseFeatureTable(partition: Iterator[Row], insertSql: String, params: Config): Unit = {

    var batchCount = 0 // 记录批量的个数
    var batchSize = 2000 // 极限个数
    var lastBatchTime = System.currentTimeMillis() // 当前开始产生批量的时间

    //1. 获取到数据源
    val dataSource: ClickHouseDataSource = ClickHouseUtils.getDataSource(params.username, params.password, params.url)
    val connection: ClickHouseConnection = dataSource.getConnection
    var ps: PreparedStatement = connection.prepareStatement(insertSql)
    //2. 给sql的占位符赋值
    partition.foreach(line => {
      var lineIndex = 1
      line.schema.fields.foreach(field => {
        field.dataType match {
          case StringType => ps.setString(lineIndex, line.getAs[String](field.name))
          case LongType => ps.setLong(lineIndex, line.getAs[Long](field.name))
          case IntegerType => ps.setInt(lineIndex, line.getAs[Int](field.name))
          case DoubleType => ps.setDouble(lineIndex, line.getAs[Double](field.name))
          case _ => logger.error(s"type is error : ${field.dataType}")
        }
        lineIndex += 1
      })

      ps.addBatch() // 把ps添加到批处理集合中
      batchCount += 1
      if (batchCount >= batchSize || lastBatchTime < System.currentTimeMillis() - 3000) {
        lastBatchTime = System.currentTimeMillis()
        ps.executeBatch() // 执行批处理集合中
        logger.warn(s"send data to clickhouse , batch num:${batchCount}, batch time:${System.currentTimeMillis() - lastBatchTime}")
        batchCount = 0
      }
    })
    ps.executeBatch()
    logger.warn(s"send data to clickhouse , batch num:${batchCount}, batch time:${System.currentTimeMillis() - lastBatchTime}")
    ps.close()
    connection.close()
  }
}