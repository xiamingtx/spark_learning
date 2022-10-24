package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - glom

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // [1, 2], [3, 4]
    // 最大值 [2], [4]
    // 2 + 4 = 6
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = glomRDD.map(_.max)

    println(maxRDD.collect().sum)
    sc.stop()
  }
}
