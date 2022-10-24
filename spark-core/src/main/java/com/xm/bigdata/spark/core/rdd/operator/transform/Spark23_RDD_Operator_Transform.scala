package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型) leftOuterJoin    rightOuterJoin
    val rdd1 = sc.makeRDD(
      List(("a", 1), ("b", 2) // , ("c", 3)
    ))
    val rdd2 = sc.makeRDD(
      List(("a", 4), ("b", 5), ("c", 6), ("c", 7)
    ))

    // cogroup: connect + group (分组+连接)
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    /*
    (a,(CompactBuffer(1),CompactBuffer(4)))
    (b,(CompactBuffer(2),CompactBuffer(5)))
    (c,(CompactBuffer(),CompactBuffer(6, 7)))
     */
    cgRDD.collect.foreach(println)

    sc.stop()
  }
}
