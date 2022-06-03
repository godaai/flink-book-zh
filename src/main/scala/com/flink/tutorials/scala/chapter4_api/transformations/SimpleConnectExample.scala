package com.flink.tutorials.scala.api.transformations

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object SimpleConnectExample {

  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val intStream: DataStream[Int] = senv.fromElements(1, 0, 9, 2, 3, 6)
    val stringStream: DataStream[String] = senv.fromElements("LOW", "HIGH", "LOW", "LOW")

    val connectedStream: ConnectedStreams[Int, String] = intStream.connect(stringStream)

    val mapResult = connectedStream.map(new MyCoMapFunction)

    senv.execute("simple connect transformation")

  }

  // CoMapFunction三个泛型分别对应第一个流的输入、第二个流的输入，map()之后的输出
  class MyCoMapFunction extends CoMapFunction[Int, String, String] {

    override def map1(input1: Int): String = input1.toString

    override def map2(input2: String): String = input2
  }
}
