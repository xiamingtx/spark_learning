package com.xm.bigdata.spark.core.framework.common

import com.xm.bigdata.spark.core.framework.controller.WordCountController
import com.xm.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
trait TApplication {

  def start(master: String = "local[*]", app: String = "Application")(op: => Unit) = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sc.stop()
    EnvUtil.clear()
  }
}
