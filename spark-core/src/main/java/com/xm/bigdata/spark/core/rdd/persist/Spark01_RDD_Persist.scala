package com.xm.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect.foreach(println)

    println("======================")

    val list1 = List("Hello Scala", "Hello Spark")

    val rdd1 = sc.makeRDD(list1)

    val flatRDD1 = rdd1.flatMap(_.split(" "))

    val mapRDD1 = flatRDD1.map((_, 1))

    val groupRDD = mapRDD1.groupByKey()

    groupRDD.collect.foreach(println)
    /*
    (Spark,1)
    (Hello,2)
    (Scala,1)
    ======================
    (Spark,CompactBuffer(1))
    (Hello,CompactBuffer(1, 1))
    (Scala,CompactBuffer(1))
     */
    sc.stop()
  }
}
