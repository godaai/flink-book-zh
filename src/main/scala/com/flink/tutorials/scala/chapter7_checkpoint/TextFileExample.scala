package com.flink.tutorials.scala.api.chapter7

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._

object TextFileExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 文件路径
    val filePath = getClass.getClassLoader.getResource("taobao/UserBehavior-20171201.csv").getPath

    // 文件为纯文本格式
    val textInputFormat = new TextInputFormat(new org.apache.flink.core.fs.Path(filePath))

    // 每隔100毫秒检测一遍
//    val inputStream = env.readFile(
//      textInputFormat,
//      filePath,
//      FileProcessingMode.PROCESS_CONTINUOUSLY,
//      100)

    // 只读一次
    val readOnceStream = env.readFile(
      textInputFormat,
      filePath,
      FileProcessingMode.PROCESS_ONCE,
      0)

    readOnceStream.print()
    env.execute("read file from path")
  }
}
