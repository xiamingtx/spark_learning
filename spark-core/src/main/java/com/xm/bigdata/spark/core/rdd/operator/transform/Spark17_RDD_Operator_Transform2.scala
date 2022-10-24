package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型)

    // 我们再来测试有 a、b两组key的情况
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    rdd.aggregateByKey(0)(_+_,_+_).collect.foreach(println)

    // 如果聚合计算时 分区内和分区间计算规则相同 spark提供了简化方法
    rdd.foldByKey(0)(_+_).collect.foreach(println)

    sc.stop()
  }
}
