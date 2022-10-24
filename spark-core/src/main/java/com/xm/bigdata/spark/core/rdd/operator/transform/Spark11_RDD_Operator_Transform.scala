package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - coalesce可以扩大分区 但是如果不进行shuffle操作 是没有意义的
    // 所以如果想要实现扩大分区的效果 必须要将shuffle设置为true
    // spark提供了简化的操作
    // 缩减分区: coalesce 如果想要数据均衡 可以采用shuffle
    // 扩大分区: repartition
    // 实际上底层代码就是 coalesce(numPartitions, shuffle = true) 而且肯定会shuffle
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

//    val newRDD: RDD[Int] = rdd.coalesce(3, shuffle = true)
    val newRDD: RDD[Int] = rdd.repartition(3)

    newRDD.saveAsTextFile("output")
    sc.stop()
  }
}
