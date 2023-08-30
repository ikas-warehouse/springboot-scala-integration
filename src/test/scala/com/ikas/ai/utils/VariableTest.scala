package com.ikas.ai.utils

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.domain.PageRequest

import com.ikas.ai.spark.VariableSet
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class VariableTest @Autowired()() {

  @Test
  def `testVariable`() {
    val startTime = System.currentTimeMillis()
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("Example")
      .master("spark://192.168.11.200:7077")
      .config("spark.driver.host", "10.0.2.21")
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
    val vs = new VariableSet(df, input_variables = List("id"), output_variables = List("name"), keep_variables = List("age"))
    vs.run()
    val outputs: mutable.Map[String, Any] = vs.getOutputs()

    val result = outputs.getOrElse("table_name", spark.emptyDataFrame).asInstanceOf[DataFrame]

    val (input, output, keep) = com.ikas.ai.utils.metadataRole.getColRoleListFromRoleMetadata(result)
    println(input)
    println(output)
    println(keep)

    println("--------------------------------------------------")
    result.show()
    println("--------------------------------------------------")

    val endTime = System.currentTimeMillis()

    println((endTime - startTime))
  }

}