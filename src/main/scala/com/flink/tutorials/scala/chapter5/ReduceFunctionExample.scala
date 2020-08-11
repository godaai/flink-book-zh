package com.flink.tutorials.scala.api.chapter5

import com.flink.tutorials.scala.utils.stock.{StockPrice, StockSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ReduceFunctionExample {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stockStream: DataStream[StockPrice] = senv.addSource(new StockSource("stock/stock-tick-20200108.csv"))

    // reduce的返回类型必须和输入类型StockPrice一致
    val sum = stockStream
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .reduce((s1, s2) => StockPrice(s1.symbol, s2.price, s2.ts,s1.volume + s2.volume))

    sum.print()

    senv.execute("window reduce function")
  }

}
