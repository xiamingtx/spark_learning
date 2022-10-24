package com.xm.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 2. 创建RDD
    // 从内存中创建RDD 将内存中集合的数据作为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4)

    // parallelize并行
//    val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq)
    // makeRDD在底层实现时其实就是调用了rdd对象的parallelize方法

    rdd.collect().foreach(println)
    // 3. 关闭环境
    sc.stop()
  }
}
