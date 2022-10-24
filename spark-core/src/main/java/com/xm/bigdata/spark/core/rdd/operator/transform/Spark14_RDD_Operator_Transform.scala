package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - (key-value类型)
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    // RDD => PairRDDFunctions
    // 隐式转换 (二次编译)
    // implicit def rddToPairRDDFunctions
    // partitionBy根据指定的分区规则对数据进行重新分区
    // Spark默认提供了HashPartitioner
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))

    // 思考一个问题:如果重分区的分区器和当前RDD的分区器一样怎么办
    // 思考一个问题:Spark还有其他分区器吗?
    // 思考一个问题:如果想按照自己的方法进行数据分区怎么办?
//    newRDD.partitionBy(new HashPartitioner(2))
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
