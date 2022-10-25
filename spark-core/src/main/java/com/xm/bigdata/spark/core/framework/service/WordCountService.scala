package com.xm.bigdata.spark.core.framework.service

import com.xm.bigdata.spark.core.framework.common.TService
import com.xm.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 *
 *
 * @author 夏明
 * @version 1.0
 */
/*
* 服务层
* */
class WordCountService extends TService{
  private val wordCountDao = new WordCountDao()

  // 数据分析
  def dataAnalysis() = {
    val lines = wordCountDao.readFile("datas/word.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word, 1)
    )

    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordCount.collect()
    array
  }
}
