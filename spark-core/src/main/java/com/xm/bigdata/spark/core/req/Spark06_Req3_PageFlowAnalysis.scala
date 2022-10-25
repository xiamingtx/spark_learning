package com.xm.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark06_Req3_PageFlowAnalysis {
  def main(args: Array[String]): Unit = {
    // Top10 热门品类
    val sparkConf = new SparkConf().setMaster("local").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)

    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong,
        )
      }
    )
    actionDataRDD.cache()

    // 对指定的页面连续跳转进行统计
    // 1-2 2-3 3-4 4-5 5-6 6-7
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okFlowIds: List[(Long, Long)] = ids.zip(ids.tail)

    // 1. 计算分母
    val pageIdToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => ids.init.contains(action.page_id) // init不包含最后一个
    ).map(
      action => (action.page_id, 1L)
    ).reduceByKey(_+_).collect().toMap
    // 2. 计算分子
    // 根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    // 分组后 根据访问时间进行排序(升序)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        // [1, 2, 3, 4]
        // [1, 2], [2, 3], [3, 4]
        // [1-2, 2-3, 3-4]
        // Sliding: 滑窗
        // [1, 2, 3, 4]
        // [2, 3, 4]
        // zip: 拉链
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        // 将不合法的页面跳转进行过滤
        pageFlowIds.filter(
          tuple => okFlowIds.contains(tuple)
        ).map(
          t => (t, 1)
        )
      }
    )
    // ((1, 2), 1)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    // ((1, 2), sum)
    val dataRDD = flatRDD.reduceByKey(_+_)

    // 计算单跳转换率
    // 分子 / 分母
    dataRDD.foreach{
      case ((pageId1, pageId2), sum) =>
        val lon: Long = pageIdToCountMap.getOrElse(pageId1, 0L)
        println(s"页面${pageId1}跳转到页面${pageId2}单跳转换率为:" + (sum.toDouble / lon))
    }

    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的ID
    session_id: String,//Session的ID
    page_id: Long,//某个页面的ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的ID
    click_product_id: Long,//某一个商品的ID
    order_category_ids: String,//一次订单中所有品类的ID集合
    order_product_ids: String,//一次订单中所有商品的ID集合
    pay_category_ids: String,//一次支付中所有品类的ID集合
    pay_product_ids: String,//一次支付中所有商品的ID集合
    city_id: Long
  )//城市 id
}
