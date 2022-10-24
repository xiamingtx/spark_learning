package com.xm.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 2. 创建RDD
    // textFile可以将文件作为数据处理的数据源 默认也可以设定分区
    // minPartitions 最小分区数量
    // math.min(defaultParallelism, 2)
    // 如果不想使用默认的分区数量 可以通过第二个参数进行指定
    // 分区数量的真正计算方式:
    // Spark读取文件底层是用hadoop
    // getSplits中有totalSize 对所有文件统计字节数的总和
    // long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits);
    // 例： totalSize = 7   goalSize = 7 / 2 = 3(byte)
    // 7 / 3 = 2...1 (1.1) + 1 = 3(分区)
//    val rdd = sc.textFile("datas/1.txt")
    val rdd = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")
    // 3. 关闭环境
    sc.stop()
  }
}
