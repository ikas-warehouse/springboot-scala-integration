package com.ikas.ai.spark

import com.ikas.ai.utils.metadataRole.insertRoleMetaData
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable


class VariableSet(table_name: DataFrame, input_variables: List[String] = List(),
                  output_variables: List[String] = List(), keep_variables: List[String] = List()) extends SparkBaseOperator {


  // 参数初始

  // 入参审查
  assert(table_name.isInstanceOf[DataFrame], "输入数据应为数据框，请检查")
  assert(table_name.count() > 0, "输入数据不能为空，请检查")
  assert(input_variables.nonEmpty, "输入参数不能为空，请检查")
  // assert(outputVariables.nonEmpty, "输出参数不能为空，请检查")
  // assert(keepVariables.nonEmpty, "时间参数不能为空，请检查")
  // assert(outputVariables.length <= 1, "输出参数过多，请检查")  // 可为空列表或有一个输出变量
  if (output_variables.length > 1) {
    throw new IllegalArgumentException("输出参数过多，请检查!")
  }


  def run(): Unit = {
    val data = insertRoleMetaData(table_name, inputCols = input_variables, outputCols =output_variables, keepCols=keep_variables)
    outputs("table_name") = data

  }
}
//
private object VariableSet {

  private def main(args:Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("Example")
      .master("spark://192.168.11.200:7077")
      .config("spark.driver.host", "192.168.28.49")
      .getOrCreate()

    // 创建一个示例DataFrame
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))

    val data = Seq(
      Row(1, "Alice", 25),
      Row(2, "Bob", 30),
      Row(3, "Charlie", 35)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

//    输入数据参数
//    val result = insertRoleMetaData(df, inputCols = List("id"), outputCols = List("name"), keepCols = List("age"))
    val vs = new VariableSet(df, input_variables = List("id"), output_variables= List("name"), keep_variables = List("age") )
    vs.run()
    val outputs:mutable.Map[String, Any] = vs.getOutputs()

// 输出结果
//    val result = outputs.get("table_name") match {
//      case  Some(data: DataFrame) => data //
//      case _ => spark.emptyDataFrame //
//    }
    val result:DataFrame = outputs.getOrElse("table_name", spark.emptyDataFrame).asInstanceOf[DataFrame] // Any 转DataFrame
//    println(result.show())
//    println(result.isInstanceOf[DataFrame])

//    val metadataDict = getRoleMetadata(result)


//    metadataDict.foreach { case (key, value) => {
//      println(s"Key: $key, value:$value")
//    }
//    }
    result

  }
}
