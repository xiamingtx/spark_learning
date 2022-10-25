package com.xm.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    // Top10 热门品类
    val sparkConf = new SparkConf().setMaster("local").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action.txt")
    // 2. 统计品类点击数量: (品类ID, 点击数量)
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类下单数量: (品类ID, 下单数量)
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    // orderid => 1, 2, 3
    // [1, 2, 3]
    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_+_)
    // 4. 统计品类支付数量: (品类ID, 支付数量)
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    // orderid => 1, 2, 3
    // [1, 2, 3]
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_+_)

    // 5. 将品类进行排序 并且取前十名
    // 点击数量排序, 下单数量排序, 支付数量排序
    // 元组排序: 先比较第一个 再比较第二个 再比较第三个 以此类推
    // (品类ID, (点击数量, 下单数量, 支付数量))
    // join  要求点击数量和下单数量等等都得有数据 排除
    // zip 跟数量与位置有关系 跟品类ID无法关联 排除
    // leftOuterJoin 无法确定左表 也不合适
    // cogroup 即使数据源中没有数据也会做组连接 connect + group

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues{
      case (clickIter, orderIter, payIter) =>
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if ( iter1.hasNext ) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if ( iter2.hasNext ) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if ( iter3.hasNext ) {
          payCnt = iter3.next()
        }

        ( clickCnt, orderCnt, payCnt )
    }

    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)
    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }
}
