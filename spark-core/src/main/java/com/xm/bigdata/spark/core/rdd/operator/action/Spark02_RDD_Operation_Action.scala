package com.xm.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_RDD_Operation_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 行动算子
    // reduce
    val i: Int = rdd.reduce(_ + _)
    println(i) // 10

    // collect: 方法会将不同分区的数据按照分区顺序采集到Driver端内存中 形成数组
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(", "))

    // count: 数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    // first 获取数据源中第一个数据
    val first = rdd.first()
    println(first)

    // take 获取n个数据
    val ints1: Array[Int] = rdd.take(3)
    println(ints1.mkString(", "))

    // takeOrdered 数据排序后 取N个数据
    val rdd1 = sc.makeRDD(List(4, 2, 3, 1))
//    val ints2: Array[Int] = rdd1.takeOrdered(3)
    val ints2: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
    println(ints2.mkString(", "))

    sc.stop()
  }
}
