package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - mapPartitions

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // [1, 2], [3, 4]
    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        // 1 2 3 4
        // 想要: (2, 1) (5, 2) (8, 3) (11, 4) 前面分区号 后面是值
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
