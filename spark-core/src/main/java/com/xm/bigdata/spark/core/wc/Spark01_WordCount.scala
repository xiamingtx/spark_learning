package com.xm.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark框架
    // 1. 建立和Spark框架的连接
    // JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 2. 执行业务操作
    // 1) 读取文件 获取一行一行的数据
    // hello world
    val lines: RDD[String] = sc.textFile("E:\\workspace\\JavaProject\\xm-spark\\datas")
    // 2) 将一行数据进行拆分 形成一个个的单词 (分词)
    /// 扁平化 将整体拆分成个体
    // "hello world" => hello, world
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 3) 将数据根据单词进行分组 便于统计 (相同的单词放一块儿)
    // (hello, hello, hello)   (world, world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    // 4) 对分组后的数据进行转换
    // (hello, hello, hello)   (world, world)
    // (hello, 3)  (world, 2)
    val wordCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5) 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)
    // 3. 关闭连接
    sc.stop()
  }
}
