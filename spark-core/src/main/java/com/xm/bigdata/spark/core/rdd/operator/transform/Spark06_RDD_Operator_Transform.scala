package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - groupBy

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // groupBy 将数据源中每一个数据进行分组判断 我们根据返回的分组key进行分组
    // 相同的key的数据会放在同一个分组中
    def groupByFunction(num: Int): Int = {
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupByFunction)

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
