package com.xm.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//    sparkConf.set("spark.default.parallelism", "5") // 也可以进行配置
    val sc = new SparkContext(sparkConf)
    // 2. 创建RDD
    // [1, 2], [3, 4]
    // val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // [1] [2] [3, 4]
    // val rdd = sc.makeRDD(List(1, 2, 3, 4), 3)
    // [1] [2, 3] [4, 5]
    // val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    rdd.saveAsTextFile("output")
    // 3. 关闭环境
    sc.stop()
  }
}
