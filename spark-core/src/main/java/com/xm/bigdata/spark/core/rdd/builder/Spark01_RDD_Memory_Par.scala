package com.xm.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//    sparkConf.set("spark.default.parallelism", "5") // 也可以进行配置
    val sc = new SparkContext(sparkConf)
    // 2. 创建RDD
    // rdd的并行度 & 分区
    // makeRDD方法可以传递第二个参数 这个参数表示有多少个分区
    // 第二个参数可以不传 那么makeRDD方法会使用默认值 defaultParallelism(默认并行度)
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // spark在默认情况下 从配置对象中获取配置参数 spark.default.parallelism
    // 如果获取不到 那么使用totalCores属性 这个属性取值为当前运行环境的最大可用核数
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )

    val rdd = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")
    // 3. 关闭环境
    sc.stop()
  }
}
