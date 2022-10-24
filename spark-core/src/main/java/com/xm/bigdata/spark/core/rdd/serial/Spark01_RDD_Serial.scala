package com.xm.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")

    // NotSerializableException: com.xm.bigdata.spark.core.rdd.serial.Spark01_RDD_Serial$Search
//    search.getMatch1(rdd).collect().foreach(println)
    search.getMatch2(rdd).collect().foreach(println) // 同上
    sc.stop()
  }

  // 查询对象
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测 实际上就等同于类进行闭包检测
  class Search (query: String) {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      // 一种方法是混入特征 extends Serializable
      // 另一种是用另一个值来存query

//      rdd.filter(x => x.contains(query))
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }
}
