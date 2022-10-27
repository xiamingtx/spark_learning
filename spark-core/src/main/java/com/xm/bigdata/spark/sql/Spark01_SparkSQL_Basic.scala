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
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {

    // 1. 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 在使用DataFrame时 如果涉及到转换操作 需要使用转换规则
    // 注意 我们这里spark是上面的对象
    import spark.implicits._

    // 2. 执行逻辑操作

    // DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    df.show()

    // DataFrame => SQL
    df.createTempView("user")

    spark.sql("select * from user").show
    spark.sql("select age, username from user").show
    spark.sql("select avg(age) from user").show

    // DataFrame => DSL
    df.select("age", "username").show
    df.select($"age" + 1).show
    df.select('age + 1).show
    // Dataset
    // DataFrame其实是特定泛型的Dataset
    val seq = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    // RDD <=> DataFrame
    val rdd1 = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df1: DataFrame = rdd1.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df1.rdd

    // DataFrame <=> DataSet
    val ds1: Dataset[User] = df1.as[User]
    val df2: DataFrame = ds1.toDF()
    // RDD <=> DataSet
    val ds2: Dataset[User] = rdd1.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

    val userRDD: RDD[User] = ds2.rdd
    // 3. 关闭环境
    spark.close()
  }
  case class User(id: Int, name: String, age: Int)
}
