package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - glom

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // List => Int
    // Int => Array
    // 一个分区的数据形成一个glom
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    /*
    1, 2
    3, 4
     */
    glomRDD.collect().foreach(data => println(data.mkString(", ")))
    sc.stop()
  }
}
