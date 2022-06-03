package com.flink.tutorials.scala.api.transformations

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.util.Collector

object FlatMapExample {

  def main(args: Array[String]): Unit = {

    // 创建 Flink 执行环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = senv.fromElements("Hello World", "Hello this is Flink")

    // split函数的输入为 "Hello World" 输出为 "Hello" 和 "World" 组成的列表 ["Hello", "World"]
    // flatMap()将列表中每个元素提取出来
    // 最后输出为 ["Hello", "World", "Hello", "this", "is", "Flink"]
    val words: DataStream[String] = dataStream.flatMap ( input => input.split(" ") )

    val words2: DataStream[String] = dataStream.flatMap{ _.split(" ") }

    // 只对字符串数量大于15的句子进行处理
    val longSentenceWords: DataStream[String] = dataStream.flatMap {
      input => {
        if (input.size > 15) {
          // 输出是 TraversableOnce 因此返回必须是一个列表
          // 这里将Array[String]转成了Seq[String]
          input.split(" ").toSeq
        } else {
          // 为空时必须返回空列表，否则返回值无法与TraversableOnce匹配！
          Seq.empty
        }
      }
    }

    val flatMapWithStream: DataStream[String] = dataStream.flatMapWith {
      case (sentence: String) => {
        if (sentence.size > 15) {
          sentence.split(" ").toSeq
        } else {
          Seq.empty
        }
      }
    }

    val functionStream: DataStream[String] = dataStream.flatMap(new WordSplitFlatMap(10))

    val lambda: DataStream[String] = dataStream.flatMap{
      (value: String, out: Collector[String]) => {
        if (value.size > 10) {
          value.split(" ").foreach(out.collect)
        }
      }
    }

    val richFunctionStream: DataStream[String] = dataStream.flatMap(new WordSplitRichFlatMap(10))

    val jobExecuteResult: JobExecutionResult = senv.execute("basic flatMap transformation")

    // 执行结束后 获取累加器的结果
    val lines: Int = jobExecuteResult.getAccumulatorResult("num-of-lines")
    println("num of lines: " + lines)
  }

  // 使用FlatMapFunction实现过滤逻辑，只对字符串长度大于 limit 的内容进行词频统计
  class WordSplitFlatMap(limit: Int) extends FlatMapFunction[String, String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if (value.size > limit) {
        // split返回一个Array
        // 将Array中的每个元素使用Collector.collect收集起来，起到将列表展平的效果
        value.split(" ").foreach(out.collect)
      }
    }
  }

  // 使用RichFlatMapFunction实现
  // 添加了累加器 Accumulator
  class WordSplitRichFlatMap(limit: Int) extends RichFlatMapFunction[String, String] {
    // 创建一个累加器
    val numOfLines: IntCounter = new IntCounter(0)

    override def open(parameters: Configuration): Unit = {
      // 在RuntimeContext中注册累加器
      getRuntimeContext.addAccumulator("num-of-lines", this.numOfLines)
    }

    override def flatMap(value: String, out: Collector[String]): Unit = {
      // 运行过程中调用累加器
      this.numOfLines.add(1)
      if(value.size > limit) {
        value.split(" ").foreach(out.collect)
      }
    }
  }

}
