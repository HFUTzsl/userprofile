package com.qf.bigdata.profile.utils

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

/**
  * clickhouse datasource
  */
object ClickHouseUtils {

  /**
    * 连接到clickhouse,获取到数据源
    */
  def getDataSource(user:String, password:String, url:String):ClickHouseDataSource = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    val properties = new ClickHouseProperties()
    properties.setUser(user)
    properties.setPassword(password)
    val dataSource = new ClickHouseDataSource(url, properties)
    dataSource
  }

  /**
    * 将sparksql中的类型转换成为clickhouse可用的类型
    */
  def dfType2CHType(fNameType : String) : String = {
    fNameType.toLowerCase() match {
      case "string" => "String"
      case "integer" => "Int32"
      case "long" => "Int64"
      case "float" => "Float32"
      case "double" => "Float64"
      case "date" => "Date"
      case "timestamp" => "Datetime"
      case _ => "String"
    }
  }
  // uid String, gender String,
  def dfTypeName2CH(dfCol:String):String = {
    dfCol.split(",").map(line => {
      val col = line.split(" ")
      val colType = dfType2CHType(col(1))
      val name = col(0)
      name + " " + colType
    }).mkString(",")
  }

}
