package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - flatMap

    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))

    // 模式匹配 如果不是List就包装成List
    val flatRDD: RDD[Any] = rdd.flatMap {
      case list: List[_] => list
      case data => List(data)
    }
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
