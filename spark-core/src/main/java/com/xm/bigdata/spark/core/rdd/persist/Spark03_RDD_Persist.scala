package com.xm.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_RDD_Persist {
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
    // cache默认持久化的操作 只能将数据保存到内存中 如果想要保存到磁盘文件 需要更改存储级别
//    mapRDD.cache()
    // RDD对象的持久化操作 不一定是只为了重用 在数据执行较长或者比较重要的场合 也可以进行持久化
    mapRDD.persist(StorageLevel.DISK_ONLY)

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
      (Spark,CompactBuffer(1))
      (Hello,CompactBuffer(1, 1))
      (Scala,CompactBuffer(1))
     */
    sc.stop()
  }
}
