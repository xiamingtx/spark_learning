package com.xm.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 案例实操

    // 1. 获取原始数据: 时间戳 省份 城市 用户 广告
    val dataRDD = sc.textFile("datas/agent.log")
    // 2. 将原始数据进行结构的转换 方便统计
    // 时间戳 省份 城市 用户 广告
    // =>
    // ((省份, 广告), 1)
    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    // 3. 将转换后的数据进行分组聚合
    // ((省份, 广告), 1) => ((省份, 广告), sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    // 4. 将聚合的结果进行结构的转换
    // ((省份, 广告), sum) => (省份, (广告, sum))
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prv, ad), sum) => (prv, (ad, sum))
    }
    // 5. 将转换结构后的数据根据省份进行分组
    // (省份, [(广告A, sumA), (广告B, sumB)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    // 6. 将分组后的数据组内排序 (降序), 取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
    // 7. 采集数据打印在控制台
    resultRDD.collect.foreach(println)
    /*
    (4,List((12,25), (2,22), (16,22)))
    (8,List((2,27), (20,23), (11,22)))
    (6,List((16,23), (24,21), (22,20)))
    (0,List((2,29), (24,25), (26,24)))
    (2,List((6,24), (21,23), (29,20)))
    (7,List((16,26), (26,25), (1,23)))
    (5,List((14,26), (21,21), (12,21)))
    (9,List((1,31), (28,21), (0,20)))
    (3,List((14,28), (28,27), (22,25)))
    (1,List((3,25), (6,23), (5,22)))
     */
    sc.stop()
  }
}
