package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型) leftOuterJoin    rightOuterJoin
    val rdd1 = sc.makeRDD(
      List(("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2 = sc.makeRDD(
      List(("a", 4), ("b", 5), //  ("c", 6)
    ))

    val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
//    val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

    leftJoinRDD.collect.foreach(println)
    /*
      (a,(1,Some(4)))
      (b,(2,Some(5)))
      (c,(3,None))
     */

    sc.stop()
  }
}
