package com.ikas.ai.spark

abstract class SparkBaseOperator extends BaseOperator {
  this.__is_spark__ = true

}


private class TestClass extends SparkBaseOperator {

  def run(): Unit = {
    outputs("key1") = "value1"
  }

}

private object SparkBaseOperatorTest {
  def main(args: Array[String]): Unit = {

    var sbt = new TestClass()
    sbt.run()
    println(sbt.getSpark())
    println(sbt.getOutputs())
  }
}