package com.xm.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("==============")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("==============")

    val wordToOne = words.map(
      word => (word, 1)
    )
    println(wordToOne.dependencies)
    println("==============")

    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordCount.dependencies)
    println("==============")

    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
