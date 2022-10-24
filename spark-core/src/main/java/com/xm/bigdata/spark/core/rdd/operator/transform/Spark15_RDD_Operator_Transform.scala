package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型)
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // reduceByKey: 相同key的数据进行value数据据的聚合操作 隐含了一个分组的概念
    // Scala 语言中一般的聚合操作都是两两聚合 Spark是基于Scala开发的 它的聚合也是如此
    // [1, 2, 3]
    // [3, 3]
    // 6
    // reduceByKey 如果key只有一个 是不会参与运算的
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = $x, y = $y")
      x + y
    })
    /*
      x = 1, y = 2
      x = 3, y = 3
      (a,6)
      (b,4)
     */

    reduceRDD.collect.foreach(println)

    sc.stop()
  }
}
