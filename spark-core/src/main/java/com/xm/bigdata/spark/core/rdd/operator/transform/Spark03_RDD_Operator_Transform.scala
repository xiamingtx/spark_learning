package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - mapPartitions

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // [1, 2], [3, 4]
    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator // 返回一个空的迭代器
        }
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
