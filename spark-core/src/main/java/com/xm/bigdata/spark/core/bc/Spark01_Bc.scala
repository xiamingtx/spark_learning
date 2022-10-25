package com.xm.bigdata.spark.core.bc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

//    val rdd2 = sc.makeRDD(List(
//      ("a", 4),
//      ("b", 5),
//      ("c", 6)
//    ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // join会导致数据量几何增长 并且会影响shuffle的性能  不推荐使用
    // val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    /*
    (a,(1,4))
    (b,(2,5))
    (c,(3,6))
     */
    // joinRDD.collect.foreach(println)

    rdd1.map {
      case (word, count) =>
        val l: Int = map.getOrElse(word, 0)
        (word, (count, l))
    }.collect.foreach(println)

    sc.stop()
  }
}
