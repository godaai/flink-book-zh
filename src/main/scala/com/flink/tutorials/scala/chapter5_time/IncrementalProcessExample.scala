package com.flink.tutorials.scala.api.chapter5

import com.flink.tutorials.scala.utils.stock.StockSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object IncrementalProcessExample {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val input = senv.addSource(new StockSource("stock/stock-tick-20200108.csv"))

    // reduce的返回类型必须和输入类型相同
    // 为此我们将StockPrice拆成一个四元组 (股票代号，最大值、最小值)
    val maxMin = input
      .map(s => (s.symbol, s.price, s.price))
      .keyBy(s => s._1)
      .timeWindow(Time.seconds(10))
      .reduce(
        ((s1: (String, Double, Double), s2: (String, Double, Double)) => (s1._1, Math.max(s1._2, s2._2), Math.min(s1._3, s2._3))),
        new WindowEndProcessFunction
      )

    maxMin.print()

    senv.execute("combine reduce and process function")
  }

  case class MaxMinPrice(symbol: String, max: Double, min: Double, windowEndTs: Long)

  class WindowEndProcessFunction extends ProcessWindowFunction[(String, Double, Double), MaxMinPrice, String, TimeWindow] {

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double, Double)],
                         out: Collector[MaxMinPrice]): Unit = {
      val maxMinItem = elements.head
      val windowEndTs = context.window.getEnd
      out.collect(MaxMinPrice(key, maxMinItem._2, maxMinItem._3, windowEndTs))
    }
  }
}
