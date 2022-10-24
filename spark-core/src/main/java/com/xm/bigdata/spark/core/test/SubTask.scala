package com.xm.bigdata.spark.core.test

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
class SubTask extends Serializable {
  var datas: List[Int] = _

  var logic: Int => Int = _

  // 计算
  def compute(): List[Int] = {
    datas.map(logic)
  }
}
