package com.flink.tutorials.scala.api.transformations

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.streaming.api.scala._


object FilterExample {

  def main(args: Array[String]): Unit = {

    // 创建 Flink 执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Int] = senv.fromElements(1, 2, -3, 0, 5, -9, 8)

    // 使用 => 构造Lambda表达式
    val lambda = dataStream.filter ( input => input > 0 )

    // 使用 _ 构造Lambda表达式
    val lambda2 = dataStream.map { _ > 0 }

    // 继承RichFilterFunction

    val richFunctionDataStream = dataStream.filter(new MyFilterFunction(2))
    richFunctionDataStream.print()

    senv.execute("basic filter transformation")
  }

  // limit参数可以从外部传入
  class MyFilterFunction(limit: Int) extends RichFilterFunction[Int] {

    override def filter(input: Int): Boolean = {
      if (input > limit) {
        true
      } else {
        false
      }
    }

  }

}
