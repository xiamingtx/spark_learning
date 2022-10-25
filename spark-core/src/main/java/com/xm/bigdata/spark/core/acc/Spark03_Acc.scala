package com.xm.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

    val mapRDD = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 少加: 转换算子中调用累加器 如果没有行动算子的话 那么不会执行
    println(sumAcc.value) // 0
    // 多加:
    mapRDD.collect
    mapRDD.collect
    println(sumAcc.value) // 20

    // 一般情况下 我们累加器会放在行动算子中进行操作

    sc.stop()
  }
}
