package com.ikas.ai.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkILoopOptimizedExample")
      .setMaster("local") // 在本地模式运行，根据需要修改

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext

    // do something

    sc.stop()
  }
}
