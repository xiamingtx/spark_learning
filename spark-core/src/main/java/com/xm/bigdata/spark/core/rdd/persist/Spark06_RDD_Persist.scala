package com.xm.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
object Spark06_RDD_Persist {
  def main(args: Array[String]): Unit = {
    // cache: 将数据临时存储在内存中进行数据重用
    //    会在血缘关系中添加新的依赖 一旦出现了问题 可以从头读取数据
    // persist: 将数据临时存储在磁盘文件中进行数据重用
    //    涉及到磁盘IO 性能较低 但是数据安全
    //    如果作业执行完毕 临时保存的数据文件将会丢失
    // checkpoint: 将数据长久地保存在磁盘文件中进行数据重用
    //    涉及到磁盘IO 性能较低 但是数据安全
    //    为了保证数据安全 所以一般情况下 会独立执行作业
    //    为了能够提高效率 一般情况下 是需要和cache联合使用的
    //    执行过程中会切断血缘关系 重新建立新的血缘
    //    checkpoint等同于改变数据源

    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      (word, 1)
    })

//    mapRDD.cache()
     mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect.foreach(println)

    println("======================")

    println(mapRDD.toDebugString)
    /*
    (1) MapPartitionsRDD[2] at map at Spark06_RDD_Persist.scala:33 [Memory Deserialized 1x Replicated]
     |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_Persist.scala:31 [Memory Deserialized 1x Replicated]
     |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_Persist.scala:29 [Memory Deserialized 1x Replicated]
    (Spark,1)
    (Hello,2)
    (Scala,1)
    ======================
    (1) MapPartitionsRDD[2] at map at Spark06_RDD_Persist.scala:33 [Memory Deserialized 1x Replicated]
     |       CachedPartitions: 1; MemorySize: 368.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
     |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_Persist.scala:31 [Memory Deserialized 1x Replicated]
     |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_Persist.scala:29 [Memory Deserialized 1x Replicated]

     */

    /*
    (1) MapPartitionsRDD[2] at map at Spark06_RDD_Persist.scala:34 []
     |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_Persist.scala:32 []
     |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_Persist.scala:30 []
    (Spark,1)
    (Hello,2)
    (Scala,1)
    ======================
    (1) MapPartitionsRDD[2] at map at Spark06_RDD_Persist.scala:34 []
     |  ReliableCheckpointRDD[4] at collect at Spark06_RDD_Persist.scala:43 []
     */
    sc.stop()
  }
}
