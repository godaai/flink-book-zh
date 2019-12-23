package com.flink.tutorials.api.transformations

import org.apache.flink.streaming.api.scala._

object KeyByExample {

  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[(Int, Double)] = senv.fromElements((1, 1.0), (2, 3.2), (1, 5.5), (3, 10.0), (3, 12.5))

    // 使用数字位置定义Key 按照第一个字段进行分组
    val keyedStream = dataStream.keyBy(0).sum(1)

    keyedStream.print()

    senv.execute("basic keyBy transformation")

  }

}
