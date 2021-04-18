package com.qf.bigdata.profile.utils

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql._

/**
  * @Author shanlin
  * @Date Created in  2021/4/2 18:26
  *
  */

object TestPGJdbc3 {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val spark = new sql.SparkSession
  .Builder()
    .appName("source_data_mysql001")
    .config("spark.sql.crossJoin.enabled", "true")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val df1: DataFrame = getPGDF("test", "zsl.xinxi")
    //    df1.show()
    println("==========分隔线============")
    val df2: DataFrame = getPGDF("test", "zsl.info")
    //    df2.show()

    val df3: DataFrame = df1.join(df2, df1("name") === df2("姓名"))
    //    df3.show()
    //    df3.select("name","体重").filter(df3("体重")>100).show()
    //    df3.show()
    val df4: DataFrame = df3.withColumn("newCol", df3("age") + df3("体重"))
    //    df4.show()
    //    df4.filter(df4("newCol") > 120).show()
    //    df4.select("name").where(df4("newCol")===150).show()
    df4.select("*")
      //      .where("newCol = 150")
      //      .orderBy(df4("newCol").desc)
      .orderBy("newCol")
      .orderBy("身高")
    //      .show()
    df1.createTempView("t1")
    df2.createTempView("t2")

    //    spark.sql("select * from t1 where name = '赵善林'").show()
    //        spark.sql("select * from t1").show()
    //    spark.sql(
    //      """
    //        |create table if not exists dws_news.zsl as select * from t1
    //        |
    //      """.stripMargin)
    //    spark.sql(
    //      """
    //        |create table if not exists dws_news.xinxi as select * from t2
    //        |
    //      """.stripMargin)


    //    val df5: DataFrame = spark.sql("select * from dws_news.zsl")
    //    val df6: DataFrame = spark.sql("select * from dws_news.newtable")
    //    df6.withColumn("name1",df6("name"))
    //    df5.show()
    //    df6.show()

    val df7: DataFrame = spark.sql(
      """
select * from dws_news.zslnew
      """.stripMargin)
    df7.show()
    val df8: DataFrame = spark.sql(
      """
select * from dws_news.newtable
      """.stripMargin)
    df8.show()


    val df9: DataFrame = spark.sql(
      """
select * from dws_news.zslnew a join dws_news.newtable b
on a.name = b .name
      """.stripMargin)
    //    df9.show()

    //    val ds1: Dataset[Row] = df7.join(df8,df7("name")===df8("name")).select("*").where(df7("name")==="zsl")
    val df10: DataFrame = spark.sql(
      """
select a.name as name1,
 a.age age,
 a.sex sex,
 b.height height,
 b.weight weight
 from dws_news.zslnew a join dws_news.newtable b
on a.name = b .name
    """.stripMargin)

    val df11: DataFrame = df10.withColumn("newCol", df10("age") + df10("height"))
    df11.createTempView("df11")
    val addAge: UserDefinedFunction = spark.udf.register("addAge", (x: Int) => {
      x + 1
    })
    println("+++++++++")
//    df11.withColumn("ccc", addAge(df11("age"))).show()
    println("+++++++++")
//    spark.sql("select name1 ,addAge(age) age,sex,height,weight,newCol from df11").show()
//    spark.sql("select name1, weight from df11 group by weight,name1 order by weight desc limit 5").show()
    System.out.println("+++++++++++++++++++++++++++++")
//    df11.select("*").show()
    System.out.println("+++++++++++++++++++++++++++++")
    val df12: DataFrame = df11.selectExpr("name1 as name", "addAge(age) as age", "height as height", "weight as weight", "sex as sex", "newCol as newCol")
    //      .selectExpr("age as age")
    //    df12.select(df12("name"),addAge(df12("age"))).show()
    df12.select("name", "age").show()
    df12.show()
    df12.createTempView("df12")
    val df13: DataFrame = spark.sql(
      """
select name, case when sex='man' then '男' when sex='woman' then
'女' end as sex,height,weight,newCol from df12
      """.stripMargin)
//    df13.select(df13("name").as("newName")).show()
    df12.select(df12("name"), addAge(df12("age")))
//    df13.selectExpr("name as name1", "sex").show(100, truncate = false)
    val pro = new java.util.Properties
    pro.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
//    pro.put("user", "default")
//    pro.put("password", "qwert")
//    df13.show()
    System.out.println("***********************************")
//    df13.write.mode(SaveMode.Append)
//      .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://192.168.10.132:8321/app_news")
//      .option("dbtable", "zsl").option("user","qf-insert").option("password","qf-001")
//      .jdbc("jdbc:clickhouse://192.168.10.132:8321/app_news",
//      "zsl",
//      pro)
    df13.show()
    val prop=new Properties()
    val ckDriver="ru.yandex.clickhouse.ClickHouseDriver"
    prop.put("driver",ckDriver)
    prop.put("user","qf-insert")
    prop.put("password","qf-001")
    df13.write.mode(saveMode="append")
      .option("batchsize", "20000")
      .option("isolationLevel", "NONE") // 设置事务
      .option("numPartitions", "1") // 设置并发
      .jdbc("jdbc:clickhouse://192.168.10.132:8321/app_news", "zsl",prop)


//    df13.write.mode(SaveMode.Append)
//      .option("batchsize", "50000")
//      .option("isolationLevel", "NONE") // 设置事务
//      .option("numPartitions", "1") // 设置并发
//      .option("user","default").option("password","qwert").option("driver","ru.yandex.clickhouse.ClickHouseDriver")
//      .jdbc("jdbc:clickhouse://192.168.10.132:8132/app_news",
//        "zsl",
//        pro)

//    df13.write.mode("append").option("batchsize", "50000").option("isolationLevel", "NONE").option("numPartitions", "1").jdbc("jdbc:clickhouse://192.168.10.132:8132/app_news","zsl"
//      ,pro)

//    spark.read
//      .format("jdbc")
//      .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://192.168.10.132:8123/app_news")
//      .option("dbtable", "user_profile_base").option("user","default").option("password","qwert")
//      .load().show()

//    val prop=new Properties()
//    val ckDriver="ru.yandex.clickhouse.ClickHouseDriver"
//    prop.put("driver",ckDriver)
//    prop.put("user","default")
//    prop.put("password","qwert")
//    df13.write.mode(saveMode="append")
//      .option("batchsize", "20000")
//      .option("isolationLevel", "NONE") // 设置事务
//      .option("numPartitions", "1") // 设置并发
//      .jdbc("jdbc:clickhouse://192.168.10.132:8123/app_news", "zsl",prop)
   //    df12.

    spark.sql(
      """
        |select hour(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'))
      """.stripMargin).show()
    spark.close()
  }

  def getPGDF(database: String, tableName: String): DataFrame = {
    val jdbc_conf: Map[String, String] = Map(
      "url" -> s"jdbc:postgresql://localhost:5432/${database}", //设置postgresql的链接地址和指定数据库
      "driver" -> "org.postgresql.Driver", //设置postgresql的链接驱动
      "dbtable" -> s"${tableName}", //获取数据所在表的名成
      "user" -> "postgres", //连接postgresql的用户
      "password" -> "88888888" //连接用户的密码
    )
    //    val session = getSparkSession()
    val dataframe: DataFrame = spark.read.format("jdbc") //设置读取方式
      .options(jdbc_conf) //放入jdbc的配置信息
      .load()
    dataframe
  }

  //  def getSparkSession(): SparkSession = {
  //    val spark = new sql.SparkSession
  //    .Builder()
  //      .appName("source_data_mysql001")
  //      .config("spark.sql.crossJoin.enabled", "true")
  //      .master("local[*]")
  //      .getOrCreate()
  //    spark
  //  }
}
