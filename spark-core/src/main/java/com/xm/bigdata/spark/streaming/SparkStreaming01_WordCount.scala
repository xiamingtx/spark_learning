package com.xm.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境对象
    // StreamingContext创建时 需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示的是批量处理的周期(采集周期)
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // 2. 中间操作

    // 获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordToCount.print()
    // 3. 关闭环境对象
    // 由于SparkStreaming采集器是长期执行的任务 所以不能直接关闭
    // 如果main方法执行完毕 应用程序也会自动结束 所以不能让main方法执行完毕
//    ssc.stop()
    // 1. 启动采集器
    ssc.start()
    // 2. 等待采集器的关闭
    ssc.awaitTermination()
  }
}
