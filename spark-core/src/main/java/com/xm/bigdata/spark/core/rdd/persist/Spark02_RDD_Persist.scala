package com.xm.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      // RDD中不存储数据
      // 如果一个RDD需要重复使用 那么需要从头再次执行来获取数据
      // RDD对象可以重用的 但是数据无法重用
      println("**************")
      (word, 1)
    })

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect.foreach(println)

    println("======================")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect.foreach(println)
    /*
    **************
    **************
    **************
    **************
    (Spark,1)
    (Hello,2)
    (Scala,1)
    ======================
    **************
    **************
    **************
    **************
    (Spark,CompactBuffer(1))
    (Hello,CompactBuffer(1, 1))
    (Scala,CompactBuffer(1))
     */
    sc.stop()
  }
}
