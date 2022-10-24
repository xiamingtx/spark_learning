package com.xm.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_RDD_Operation_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 行动算子 aggregate
    // val result = rdd.aggregate(0)(_+_,_+_) // 0
    // aggregateByKey: 初始值只会参与分区内计算
    // aggregate: 初始值会参与分区内和分区间计算
    // val result = rdd.aggregate(10)(_+_,_+_) // 40

    val result = rdd.fold(10)(_+_) // 40
    println(result)

    sc.stop()
  }
}
