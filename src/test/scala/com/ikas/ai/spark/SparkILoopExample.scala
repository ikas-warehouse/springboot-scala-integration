package com.ikas.ai.spark

import org.apache.spark.repl.SparkILoop
import org.apache.spark.{SparkConf, SparkContext}

object SparkILoopExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkILoopExample")
    val sc = new SparkContext(conf)

    // 创建SparkILoop实例
    val sparkILoop = new SparkILoop()

    // 初始化SparkILoop
    sparkILoop.createInterpreter()
    sparkILoop.initializeSpark()

    // 设置SparkContext
    //    sparkILoop.intp.s("sc", sc)

    // 执行Spark任务代码
    val code =
      """
        |val inputRDD = sc.textFile("/path/to/input/file")
        |val wordsRDD = inputRDD.flatMap(line => line.split(" "))
        |val wordCounts = wordsRDD.map(word => (word, 1)).reduceByKey(_ + _)
        |wordCounts.saveAsTextFile("/path/to/output/directory")
        |""".stripMargin

    sparkILoop.interpret(code)

    // 关闭SparkILoop和SparkContext
    sparkILoop.closeInterpreter()
    sc.stop()
  }
}
