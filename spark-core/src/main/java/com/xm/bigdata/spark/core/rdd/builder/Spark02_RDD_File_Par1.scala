package com.xm.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 2. 创建RDD
    // 数据分区的分配
    // 1. 数据以行为单位进行读取
    // spark读取文件采用的是hadoop方式读取 所以一行一行读取 和字节数没有关系
    // 2. 数据读取时是以偏移量为单位的 偏移量不会被重复读取
    /*
      1@@  => 012
      2@@  => 345
      3    => 6
     */
     // 数据分区偏移量的范围的计算
     // 0 => [0, 3] => 12
     // 1 => [3, 6] => 3
     // 2 => [6, 7] => 空
     // 三个分区 但是只有0 1号分区有数据
    val rdd = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")
    // 3. 关闭环境
    sc.stop()
  }
}
