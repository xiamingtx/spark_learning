package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - sample
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // sample算子需要传递三个参数
    // 1. withReplacement: Boolean, 第一个参数表示 抽取数据后是否将数据返回 true(返回) false(不反悔)
    // 2. fraction: Double, 第二个参数表示
    // 如果抽取不放回的场合 表示数据源中每条数据被抽取的概率
    // 如果抽取放回的场合 表示 数据源中每条数据被抽取的可能次数
    // 基准值的概念
    // 3. seed: Long = Utils.random.nextLong 第三个参数表示 抽取数据时随机算法的种子
    // 如果不传递第三个参数 那么使用当前系统时间 每次取得的值就不一样
    println(rdd.sample(
      withReplacement = false,
      0.4,
//      1
    ).collect.mkString(", "))

    sc.stop()
  }
}
