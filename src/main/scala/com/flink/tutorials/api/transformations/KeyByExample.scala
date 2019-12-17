package com.flink.tutorials.api.transformations

import org.apache.flink.streaming.api.scala._

object KeyByExample {

  def main(args: Array[String]): Unit = {
    // 创建 Flink 执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[(Int, Double)] = senv.fromElements((1, 1.0), (2, 3.2), (1, 5.5), (3, 10.0), (3, 12.5))

    val keyedStream = dataStream.keyBy(0).sum(1)

    keyedStream.print()

    senv.execute("basic keyBy transformation")
  }

}
