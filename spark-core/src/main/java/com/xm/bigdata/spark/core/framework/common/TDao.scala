package com.xm.bigdata.spark.core.framework.common

import com.xm.bigdata.spark.core.framework.util.EnvUtil

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
trait TDao {
  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
