package com.flink.tutorials.scala.api.time

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ReduceFunctionExample {

  case class StockPrice(symbol: String, price: Double)

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val socketSource = senv.socketTextStream("localhost", 9000)

    val input: DataStream[StockPrice] = socketSource.flatMap {
      (line: String, out: Collector[StockPrice]) => {
        val array = line.split(" ")
        if (array.size == 2) {
          out.collect(StockPrice(array(0), array(1).toDouble))
        }
      }
    }

    // reduce的返回类型必须和输入类型StockPrice一致
    val sum = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .reduce((s1, s2) => StockPrice(s1.symbol, s1.price + s2.price))

    sum.print()

    senv.execute("window reduce function")
  }

}
