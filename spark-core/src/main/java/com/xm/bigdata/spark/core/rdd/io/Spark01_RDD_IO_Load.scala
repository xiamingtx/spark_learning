package com.xm.bigdata.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_IO_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("output1")
    println(rdd.collect.mkString(", "))

    val rdd1 = sc.objectFile[(String, Int)]("output2")
    println(rdd1.collect.mkString(", "))

    val rdd2 = sc.sequenceFile[String, Int]("output3")
    println(rdd2.collect.mkString(", "))

    /*
    (a,1), (b,1), (c,1)
    (a,1), (b,1), (c,1)
    (a,1), (b,1), (c,1)
     */
    sc.stop()
  }
}
