package com.xm.bigdata.spark.core.test

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
class Task extends Serializable {
  val datas = List(1, 2, 3, 4)

  val logic: Int => Int = _ * 2
}
