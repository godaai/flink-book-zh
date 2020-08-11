package com.flink.tutorials.scala.api.types

import org.apache.flink.streaming.api.scala._

object TupleExample {

  // Scala Tuple Example
  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[(String, Long, Double)] =
      senv.fromElements(("0001", 0L, 121.2), ("0002" ,1L, 201.8),
        ("0003", 2L, 10.3), ("0004", 3L, 99.6))

    dataStream.filter(item => item._3 > 100)

    senv.execute("scala tuple")
  }

}
