package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型)

    // 我们再来测试有 a、b两组key的情况
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    /*
     reduceByKey:
       combineByKeyWithClassTag[V](
        (v: V) => v, // 第一个值不会参与运算
        func, // 表示分区内数据的处理函数
        func, // 表示分区间数据的处理函数
        partitioner)

     aggregateByKey:
      combineByKeyWithClassTag[U](
        (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行了分区内的数据操作
        cleanedSeqOp, // 表示分区内数据的处理函数
        combOp, // 表示分区间数据的处理函数
        partitioner)

     foldByKey:
      combineByKeyWithClassTag[V](
        (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行了分区内的数据操作
        cleanedFunc, // 表示分区内数据的处理函数
        cleanedFunc, // 表示分区间数据的处理函数
        partitioner)

     combineByKey:
      combineByKeyWithClassTag(
        createCombiner, // 相同key的第一条数据的处理
        mergeValue, // 表示分区内数据的处理函数
        mergeCombiners, // 表示分区间数据的处理函数
        partitioner,
        mapSideCombine,
        serializer)(null)
     */
    rdd.reduceByKey(_+_) // wordcount
    rdd.aggregateByKey(0)(_+_, _+_) // wordcount
    rdd.foldByKey(0)(_+_) // wordcount
    rdd.combineByKey(num => num, (x: Int, y) => x + y, (x: Int, y: Int) => x + y) // wordcount
    sc.stop()
  }
}
