package com.xm.bigdata.spark.core.framework.controller

import com.xm.bigdata.spark.core.framework.common.TController
import com.xm.bigdata.spark.core.framework.service.WordCountService

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
* 控制层
* */
class WordCountController extends TController {
  private val wordCountService = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    // 执行业务层操作
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
