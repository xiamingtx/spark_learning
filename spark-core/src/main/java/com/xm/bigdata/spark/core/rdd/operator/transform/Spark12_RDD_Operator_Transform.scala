package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - sortBy
//    val rdd = sc.makeRDD(List(6, 1, 3, 5, 4, 2), 2)
    val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    // sortBy 方法可以根据制定的规则对数据源中的数据进行排序 默认为升序 第二个参数可以改变排序的方式
    // sortBy默认情况下 不会改变分区 但是中间存在shuffle操作
    val newRDD = rdd.sortBy(t => t._1.toInt, ascending = false)

    newRDD.collect.foreach(println)
    sc.stop()
  }
}
