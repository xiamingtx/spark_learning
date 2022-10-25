package com.xm.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // reduce 分区内计算 分区间计算
    // val i: Int = rdd.reduce(_+_)
    // println(i) // 10

    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println("sum = " + sum) // 0

    sc.stop()
  }
}
