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
object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    // 设定检查点路径
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("**************")
      (word, 1)
    })

    // checkpoint需要落盘 需要指定检查点保存路径
    // 检查点路径中保存的文件 当作业执行完毕后 也不会被删除
    // 一般保存路径都是在分布式存储系统: HDFS中
    // 这里为了方便就在设置在本地
    mapRDD.checkpoint()

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
