package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型) join
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 4), ("c", 5), ("a", 6)))
    // join: 两个不同数据源的数据 相同的key会连接在一起 形成元组 (类似内连接)
    // 如果两个数据源中key没有匹配上 那么数据不会出现在结果中
    // 如果两个数据源中key有多个相同的 会依次匹配 可能会出现笛卡尔乘积 数据量会几何级增长 数据量会很大
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect.foreach(println)

    sc.stop()
  }
}
