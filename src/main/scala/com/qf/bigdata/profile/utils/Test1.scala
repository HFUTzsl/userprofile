package com.qf.bigdata.profile.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @Author shanlin
  * @Date Created in  2021/4/1 23:33
  *
  */
object Test1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TEST")
//    val ss = new SparkSession(conf)
    val sc = new SparkContext(conf)
    val list = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
    val listRDD: RDD[(String, Int)] = sc.parallelize(list,2)
    listRDD.aggregateByKey(0)(math.max(_,_),_+_).foreach(println)
  }
}
