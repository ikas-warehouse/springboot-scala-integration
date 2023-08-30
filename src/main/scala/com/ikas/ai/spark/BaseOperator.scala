package com.ikas.ai.spark

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map


abstract class BaseOperator {
  // 实现算子基类

  var __is_spark__ = false
  var spark_session: Option[SparkSession] = None // spark session
//  var spark_session: SparkSession;
  var outputs = Map[String, Any]()

  def setSpark(spark: SparkSession): Unit = {
    spark_session = Some(spark)
  }

  def getSpark() : SparkSession = {

    assert(!spark_session.isEmpty, "spark session 没有被赋值，请先使用setSpark设置")
    spark_session.get

  }

  def run(): Unit

  def getOutputs(): Map[String, Any] = {
    outputs
  }


  def is_spark(): Boolean = {
    __is_spark__
  }

}
