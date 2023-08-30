package com.ikas.ai.spark

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
// 回归预测

class PredictRegresser(table_name:DataFrame, model_path:String, model:PipelineModel) extends SparkBaseOperator {

  override def run(): Unit = {
    val result = model.transform(table_name)
    // todo:reuslt 过滤掉vector UDT类型
    outputs("table_name") = result
    outputs("score_table_name") = None
  }
}

object PredictRegresser{
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder.appName("回归预测")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.driver.memory", "1024m")
      .config("spark.driver.cores", "4")
      .config("spark.executor.memory", "1024m")
      .config("spark.executor.cores", "1")
      .config("spark.cores.max", "2")
      .config("spark.driver.host", "192.168.28.49")
      .master("spark://192.168.11.200:7077,192.168.11.201:7077")
      .appName("local[4]")
      .getOrCreate()

    val start_time = System.currentTimeMillis()
    val model = PipelineModel.load("hdfs://192.168.11.200:8020/user/xian.huafeng/rfr_model3")
    val end_time = System.currentTimeMillis()
    println("scala 读取hdfs模型文件耗时", (end_time - start_time) / 1000, "s")
//    println(model)

    val data = Seq(
      Row(1.0f, 2.0f, 3.0f, 4.0f, 5.0f),
      Row(2.0f, 3.0f, 4.0f, 5.0f, 6.0f),
      Row(3.0f, 4.0f, 5.0f, 6.0f, 7.0f)
    )

    val schema = StructType(Seq(
      StructField("x1", FloatType, nullable = false),
      StructField("x2", FloatType, nullable = false),
      StructField("x3", FloatType, nullable = false),
      StructField("x4", FloatType, nullable = false),
      StructField("y", FloatType, nullable = false)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val new_df = com.ikas.ai.utils.metadataRole.insertRoleMetaData(df, inputCols = List("x1", "x2", "x3", "x4"), outputCols = List("y"))
    val pr = new PredictRegresser(table_name=new_df, model_path = "", model = model)
    pr.run()
    pr.getOutputs().toSeq.foreach{case (k, v) => v match{
      case data:DataFrame => println(data.dtypes.toList)
      case data:String => println(k, data)
      case _ =>

    }}
  }
}
