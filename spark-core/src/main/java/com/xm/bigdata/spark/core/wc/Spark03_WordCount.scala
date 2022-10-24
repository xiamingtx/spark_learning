package com.xm.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

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

    wordCount1(sc)

    sc.stop()
  }

  // groupBy
  def wordCount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  // groupByKey 有shuflle 性能较低
  def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  // reduceByKey
  def wordCount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)
  }

  // aggregateByKey
  def wordCount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_, _+_)
  }

  // foldByKey
  def wordCount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)
  }

  // combineByKey
  def wordCount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
  }

  // countByKey
  def wordCount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
  }

  // countByValue
  def wordCount8(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
  }

  // reduce
  def wordCount9(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))

    // [(word, count), (word, count),]
    // word => Map[(word, 1)]
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach{
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )

    println(wordCount)
  }
}
