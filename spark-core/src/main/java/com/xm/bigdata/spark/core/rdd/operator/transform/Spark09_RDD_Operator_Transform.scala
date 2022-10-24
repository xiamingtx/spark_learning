package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - distinct
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    //  (1, null), (2, null) (3, null) (4, null), (1, null) (2, null) (3, null) (4, null)
    // (1, null) (1, null)
    // (null, null) => null
    // (1, null) => 1
    val rdd1: RDD[Int]  = rdd.distinct()

    rdd1.collect.foreach(println)
    sc.stop()
  }
}
