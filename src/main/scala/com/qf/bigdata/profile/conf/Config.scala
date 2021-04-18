package com.qf.bigdata.profile.conf

import org.slf4j.LoggerFactory

/**
  *
  * @author shanlin
  * @date
  */
case class Config(env: String = "",
                  username: String = "",
                  password: String = "",
                  url: String = "",
                  cluster: String = "",
                  startDate: String = "",
                  endDate: String = "",
                  proxyUser: String = "root",
                  topK: Int = 25)

object Config {

  private val log = LoggerFactory.getLogger("Config")


  /**
    * 将args参数数据封装Config对象
    */
  def parseConfig(obj: Object, args: Array[String]): Config = {
    //1. 通过我们的类名获取到程序名
    val programName: String = obj.getClass.getSimpleName.replaceAll("\\$", "")
    //2. 获取到一个解析器，解析器解析参数
    val parser = new scopt.OptionParser[Config]("spark sql " + programName) {
      //2.1 添加使用说明
      head(programName, "v1.0")
      //2.2 给env属性赋值
      opt[String]('e', "env").required().action((x, config) => config.copy(env = x))
      opt[String]('x', "proxyUser").required().action((x, config) => config.copy(proxyUser = x))
      //2.3 匹配程序
      programName match {
        case "LabelGenerator" =>
          opt[String]('n', "username").required().action((x, config) => config.copy(username = x))
          opt[String]('p', "password").required().action((x, config) => config.copy(password = x))
          opt[String]('u', "url").required().action((x, config) => config.copy(url = x))
          opt[String]('c', "cluster").required().action((x, config) => config.copy(cluster = x))
        case "NewsContentSegment" =>
        case "NewsKeyWord" =>
          opt[Int]('k', "topK").required().action((x, config) => config.copy(topK = x))
        case "UserEmbedding" =>
          opt[String]('t', "topK").optional().action((x, config) => config.copy(startDate = x))
          opt[String]('d', "topK").optional().action((x, config) => config.copy(endDate = x))
        case _ =>
      }
    }
    parser.parse(args, Config()) match {
      case Some(conf) => conf
      case None =>
        log.error("cannot parse args")
        System.exit(-1) // 系统退出
        null
    }
  }
}
