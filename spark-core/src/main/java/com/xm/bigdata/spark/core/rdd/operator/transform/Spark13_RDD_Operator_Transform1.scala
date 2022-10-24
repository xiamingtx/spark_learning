package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - 双Value类型

    // 交集、并集、差集要求两个数据源的数据类型一致
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // 两个数据源要求分区数量要求保持一致
    // Can only zip RDDs with same number of elements in each partition
    // 两个数据源必须要求每个分区元素数量相同
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6), 2)

    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect.mkString(", "))

    sc.stop()
  }
}
