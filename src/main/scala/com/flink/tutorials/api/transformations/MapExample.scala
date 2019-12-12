package com.flink.tutorials.api.transformations

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    // 创建 Flink 执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Int] = senv.fromElements(1, 2, -3, 0, 5, -9, 8)

    // Lambda函数 =>
    val lambda = dataStream.map ( input => input * 2 ).print()

    // Lambda函数 _
    val lambda2 = dataStream.map { _ * 2}.print()

    // 继承RichMapFunction
    // 第一个参数是输入，第二个参数是输出
    class DoubleMapFunction extends RichMapFunction[Int, Int] {
      def map(in: Int):Int = { in * 2 }
    };

    // 匿名函数
    val anonymousFunction = dataStream.map {new RichMapFunction[Int, Int] {
      def map(input: Int): Int = {
        input * 2
      }
    }}.print()

    val richFunction = dataStream.map {new DoubleMapFunction()}.print()

    senv.execute("Basic Map Transformation")
  }
}
