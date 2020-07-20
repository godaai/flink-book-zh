package com.flink.tutorials.scala.api.transformations

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {

  def main(args: Array[String]): Unit = {

    // 创建 Flink 执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Int] = senv.fromElements(1, 2, -3, 0, 5, -9, 8)

    // 使用 => 构造Lambda表达式
    val lambda = dataStream.map ( input => ("lambda Input : " + input.toString + ", Output : " + (input * 2).toString) )

    // 使用 _ 构造Lambda表达式
    val lambda2 = dataStream.map { _ * 2 }

    // 继承RichMapFunction
    // 第一个泛型Int是输入类型，第二个String泛型是输出类型
    class DoubleMapFunction extends RichMapFunction[Int, String] {
      override def map(input: Int): String =
        ("overide map Input : " + input.toString + ", Output : " + (input * 2).toString)
    }

    val richFunctionDataStream = dataStream.map {new DoubleMapFunction()}

    // 匿名类
    val anonymousDataStream = dataStream.map {new RichMapFunction[Int, String] {
      override def map(input: Int): String = {
        ("overide map Input : " + input.toString + ", Output : " + (input * 2).toString)
      }
    }}

    senv.execute("basic map transformation")
  }

}
