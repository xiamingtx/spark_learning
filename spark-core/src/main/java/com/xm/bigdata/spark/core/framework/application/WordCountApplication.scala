package com.xm.bigdata.spark.core.framework.application

import com.xm.bigdata.spark.core.framework.common.TApplication
import com.xm.bigdata.spark.core.framework.controller.WordCountController
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object WordCountApplication extends App with TApplication{
  // 启动应用程序
  start(){
    val controller = new WordCountController()
    controller.dispatch()
  }
}
