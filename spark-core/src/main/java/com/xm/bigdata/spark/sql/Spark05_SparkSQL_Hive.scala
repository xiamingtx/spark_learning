package com.xm.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {

    // 1. 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-size.xml文件到classpath下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系（包含MySQL驱动）
    spark.sql("show tables").show

    // 关闭环境
    spark.close()
  }
}
