package com.xm.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    // spark框架提供了更多的功能 可以将分组和聚合使用一个方法实现
    // reduceByKey 相同key的数据 可以针对value进行reduce聚合
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
