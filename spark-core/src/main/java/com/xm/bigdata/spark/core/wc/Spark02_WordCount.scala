package com.xm.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      tuple => tuple._1
    )

    val wordCount: RDD[(String, Int)] = wordGroup.mapValues {
      tupleList => tupleList.map(_._2).sum
    }
    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
