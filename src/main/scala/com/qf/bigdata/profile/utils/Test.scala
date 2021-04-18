package com.qf.bigdata.profile.utils

/**
  * @Author shanlin
  * @Date Created in  2021/3/31 23:50
  *
  */
object Test {
  def main(args: Array[String]): Unit = {
    val flist = List(People("zhangsan", 10, "m"), People("lisi", 11, "w"), People("wangwu", 13, "m"))
    val strings: List[String] = flist.foldLeft(List[String]())((z, f) => {

      val title: String = f.gender match {
        case "m" => "mr."
        case "w" => "miss."
      }
      z :+ title +" "+ f.name + " is " + f.age + " years old"
    })
    strings

    println(strings(0))
    println(strings(1))
    println(strings(2))
  }

}


class People(val name: String, val age: Int, val gender: String)

object People {
  def apply(name: String, age: Int, gender: String): People = new People(name, age, gender)
}


