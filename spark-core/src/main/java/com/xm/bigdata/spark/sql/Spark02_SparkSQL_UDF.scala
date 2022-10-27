package com.xm.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {

    // 1. 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 在使用DataFrame时 如果涉及到转换操作 需要使用转换规则
    // 注意 我们这里spark是上面的对象
    import spark.implicits._

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })
    spark.sql("select age, prefixName(username) from user").show

    // 3. 关闭环境
    spark.close()
  }
}
