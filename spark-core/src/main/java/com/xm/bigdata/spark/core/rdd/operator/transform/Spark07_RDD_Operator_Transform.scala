package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - filter
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val filterRDD: RDD[Int] = rdd.filter(_ % 2 == 0)

    filterRDD.collect.foreach(println)

    sc.stop()
  }
}
