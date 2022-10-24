package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型)

    // 我们再来测试有 a、b两组key的情况
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    // combineByKey 需要三个参数
    // 1. 将相同key的第一个数据进行结构的转换 实现操作
    // 2. 分区内的计算规则
    // 3. 分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => num / cnt
    }

    resultRDD.collect.foreach(println)
    /*
    (b,4)
    (a,3)
     */
    
    sc.stop()
  }
}
