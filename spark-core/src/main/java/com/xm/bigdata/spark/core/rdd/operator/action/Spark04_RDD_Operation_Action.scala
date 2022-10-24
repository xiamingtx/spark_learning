package com.xm.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark04_RDD_Operation_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

//    val rdd = sc.makeRDD(List(1, 1, 1, 4), 2)

//    val intToLong: collection.Map[Int, Long] = rdd.countByValue()

//    println(intToLong) // Map(4 -> 1, 1 -> 3)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong) // Map(a -> 3)

    sc.stop()
  }
}
