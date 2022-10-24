package com.xm.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark07_RDD_Operation_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List[Int]())

    val user = new User()

    // SparkException: Task not serializable
    // NotSerializableException: com.xm.bigdata.spark.core.rdd.operator.action.Spark07_RDD_Operation_Action$User

    // rdd算子中传递的函数是会包含闭包操作的 那么就会进行检测功能
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()
  }

//  class User extends Serializable{
  // 样例类在编译时会自动混入序列化(实现可序列化接口)
//  case class User() {
  class User{
    var age: Int = 30
  }
}
