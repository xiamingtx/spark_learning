package com.xm.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Operation_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 行动算子
    // 所谓的行动算子 其实就是触发作业(Job)执行的方法
    // 底层代码调用的是环境对象的runjob方法 sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    // 底层代码会去创建 ActiveJob 并提交执行
    rdd.collect()
    sc.stop()
  }
}
