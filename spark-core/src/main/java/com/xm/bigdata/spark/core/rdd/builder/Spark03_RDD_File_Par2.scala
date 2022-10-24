package com.xm.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 2. 创建RDD

    // word.txt     14个字节
    // 14 / 2 = 7byte
    // 14 / 7 = 2(分区)

    /*
    1234567@@  => 012345678
    89@@       => 9 10 11 12
    0          => 13

    [0, 7]     => 1234567
    [7, 14]    => 890
     */
     // 如果数据源为多个文件 那么计算分区时以文件为单位进行分区
    val rdd = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")
    // 3. 关闭环境
    sc.stop()
  }
}
