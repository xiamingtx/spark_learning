package com.xm.bigdata.spark.core.bc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (word, count) =>
        val l: Int = bc.value.getOrElse(word, 0)
        (word, (count, l))
    }.collect.foreach(println)

    sc.stop()
  }
}
