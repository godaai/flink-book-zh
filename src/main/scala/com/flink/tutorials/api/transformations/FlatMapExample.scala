package com.flink.tutorials.api.transformations

import org.apache.flink.streaming.api.scala._

object FlatMapExample {

  def main(args: Array[String]): Unit = {
    // 创建 Flink 执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = senv.fromElements("Hello World", "Hello this is Flink")

    // split函数的输入为 "Hello World" 输出为 "Hello" 和 "World" 组成的列表 ["Hello", "World"]
    // flatMap将列表中每个元素提取出来
    // 最后输出为 ["Hello", "World", "Hello", "this", "is", "Flink"]
    val words = dataStream.flatMap ( input => input.split(" ") )

    val words2 = dataStream.map { _.split(" ") }

    // 只对字符串数量大于15的句子进行处理
    val longSentenceWords = dataStream.flatMap {
      input => {
        if (input.size > 15) {
          input.split(" ")
        } else {
          Seq.empty
        }
      }
    }

    senv.execute("basic flatMap transformation")
  }

}
