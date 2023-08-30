package com.ikas.ai.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


// metadata 操作:插入，获取
object metadataRole {
// 变量要用var, 常量要用val ，一定不要忘，
def addMetadataToColumns(df: DataFrame, columnsToUpdate: Seq[String], metadataTag: String = "input"): DataFrame = {

  df.select(
    df.columns.map {
      colName =>
        if (columnsToUpdate.contains(colName)) {
          col(colName).as(
            colName, new MetadataBuilder().putString("role", metadataTag).build()
          ) // 构建元信息
        }
        else {
          col(colName)
        }
    }: _*) // : _* 相当于解包
}


  def insertRoleMetaData(df: DataFrame, inputCols: List[String], outputCols: List[String] = List(), keepCols: List[String] = List()): DataFrame = {

    assert(inputCols.nonEmpty, "输入变量列表至少有一个元素")
    val input_df = addMetadataToColumns(df, inputCols.toSeq, "input")
    val output_df = addMetadataToColumns(input_df, outputCols.toSeq, "output")
    val keep_df = addMetadataToColumns(output_df, keepCols.toSeq, "keep")
    val select_cols = (inputCols ++ outputCols ++ keepCols)
    keep_df.select(select_cols.map(col): _*)

  }

  def getRoleMetadata(df:DataFrame) = {
    //
    val metadataDict: mutable.Map[String, mutable.Map[String, String]] = mutable.Map()
    val fields = df.schema.fields
    for (field <- fields){
      val colName = field.name
      val metadata =  field.metadata
      println(metadata.toString())

      val roleInfo = if (metadata.contains(key="role"))
      {
        metadata.getString("role")
      } else {
        "none"
      };

      metadataDict(colName) = mutable.Map("role" -> roleInfo)
    }
    metadataDict
  }

  def getColRoleListFromRoleMetadata(df:DataFrame)={

    val metadataDict: mutable.Map[String, mutable.Map[String, String]] = getRoleMetadata(df)
    val inputCols: ListBuffer[String] = ListBuffer()
    val outputCols: ListBuffer[String] = ListBuffer()
    val keepCols: ListBuffer[String] = ListBuffer()

    for ((key, value) <- metadataDict) {
      if (value("role") == "input") {
        inputCols += key
      } else if (value("role") == "output"){
        outputCols += key
      }else if (value("role") == "keep"){
        keepCols += key
      }
    }
    (inputCols,outputCols, keepCols)
  }


  def main(array: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder()
      .appName("Example")
      .master("local[1]")
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
    val result = insertRoleMetaData(df,inputCols=List("id"), outputCols=List("name"), keepCols=List("age"))

    val metadataDict = getRoleMetadata(result)
    metadataDict.foreach{case (key, value) => {
      println(s"Key: $key, value:$value")
    }}

    val (input, output, keep) = getColRoleListFromRoleMetadata(result)
    println(input, output, keep)

//    val jsonString =
//    val jsonString = "{\"name\":\"John\",\"age\":30,\"city\":\"New York\"}"
//    val dict = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]
//
//    println(dict)

//    val dict = Map("name" -> "John", "age" -> 30, "city" -> "New York")
//
//    println(json.toString())
//
//
//    println(json.toString())

  }
}
