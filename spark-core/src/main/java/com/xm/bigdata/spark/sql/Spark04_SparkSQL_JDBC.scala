package com.xm.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {

    // 1. 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 读取MySQL数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "xm")
      .option("dbtable", "user")
      .load()

    df.show

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "xm")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()

    // 关闭环境
    spark.close()
  }
}
