package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - 双Value类型

    // 交集、并集、差集要求两个数据源的数据类型一致
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))
    val rdd7 = sc.makeRDD(List("3", "4", "5", "6"))

    // 交集 [3, 4]
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
//    val rdd8 = rdd1.intersection(rdd7)
    println(rdd3.collect.mkString(", "))

    // 并集 [1, 2, 3, 4, 5, 6]
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect.mkString(", "))

    // 差集 [1, 2]
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect.mkString(", "))

    // 拉链: [(1, 3), (2, 4), (3, 5), (4, 6)]
    // 拉链可以数据类型不一致
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    val rdd8 = rdd1.zip(rdd7)
    println(rdd6.collect.mkString(", "))
    println(rdd8.collect.mkString(", "))

    sc.stop()
  }
}
