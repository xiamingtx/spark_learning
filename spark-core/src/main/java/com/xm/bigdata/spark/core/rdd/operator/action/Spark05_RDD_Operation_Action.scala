package com.xm.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark05_RDD_Operation_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    /// saveAsSequenceFile要求数据的格式必须为k-v类型
    rdd.saveAsSequenceFile("output2")

    sc.stop()
  }
}
