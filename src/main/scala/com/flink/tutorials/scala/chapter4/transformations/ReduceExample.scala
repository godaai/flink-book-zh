package com.flink.tutorials.scala.api.transformations

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object ReduceExample {

  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[Score] = senv.fromElements(
      Score("Li", "English", 90), Score("Wang", "English", 88), Score("Li", "Math", 85),
      Score("Wang", "Math", 92), Score("Liu", "Math", 91), Score("Liu", "English", 87))

    // 实现ReduceFunction
    val sumReduceFunctionStream: DataStream[Score] = dataStream
      .keyBy(item => item.name)
      .reduce(new MyReduceFunction)

    // 使用 Lambda 表达式
    val sumLambdaStream: DataStream[Score] = dataStream
      .keyBy(item => item.name)
      .reduce((s1, s2) => Score(s1.name, "Sum", s1.score + s2.score))

    senv.execute("basic reduce transformation")
  }

  case class Score(name: String, course: String, score: Int)

  class MyReduceFunction() extends ReduceFunction[Score] {
    // reduce 接受两个输入，生成一个同类型的新的输出
    override def reduce(s1: Score, s2: Score): Score = {
      Score(s1.name, "Sum", s1.score + s2.score)
    }
  }

}
