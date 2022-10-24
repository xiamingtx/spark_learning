package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - coalesce
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // coalesce方法默认情况下不会把分区打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡 出现数据倾斜
    // 如果想让数据均衡 我们可以进行shuffle处理
    val newRDD: RDD[Int] = rdd.coalesce(2, shuffle = true)

    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
