package com.qf.bigdata.profile.utils

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkUtils {

  private val logger = LoggerFactory.getLogger(SparkUtils.getClass);

  def getSparkSession(env: String, appName: String): SparkSession = {
    env match {
      case "prod" => {
        SparkSession.builder()
          .appName(appName + "_" + "prod")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.hive.metastore.version", "1.2.1")
          .config("spark.sql.cbo.enabled", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .enableHiveSupport()
          .getOrCreate()
      }
      case "dev" => {
        SparkSession.builder()
          .appName(appName + "_" + "dev")
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.hive.metastore.version", "1.2.1")
          .config("spark.sql.cbo.enabled", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .enableHiveSupport()
          .getOrCreate()
      }
      case _ => {
        logger.error("not match env, errors")
        System.exit(-1)
        null
      }
    }
  }
}