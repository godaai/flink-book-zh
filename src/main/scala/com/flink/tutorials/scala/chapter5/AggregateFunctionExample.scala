package com.flink.tutorials.scala.api.chapter5

import com.flink.tutorials.scala.utils.stock.{StockPrice, StockSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateFunctionExample {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val input: DataStream[StockPrice] = senv.addSource(new StockSource("stock/stock-tick-20200108.csv"));

    val average = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .aggregate(new AverageAggregate)

    average.print()

    senv.execute("window aggregate function")
  }

  /**
    * 接收三个泛型：
    * IN: StockPrice
    * ACC：(String, Double, Int) - (symbol, sum, count)
    * OUT: (String, Double) - (symbol, average)
    */
  class AverageAggregate extends AggregateFunction[StockPrice, (String, Double, Int), (String, Double)] {

    override def createAccumulator() = ("", 0, 0)

    override def add(item: StockPrice, accumulator: (String, Double, Int)) =
      (item.symbol, accumulator._2 + item.price, accumulator._3 + 1)

    override def getResult(accumulator:(String, Double, Int)) = (accumulator._1 ,accumulator._2 / accumulator._3)

    override def merge(a: (String, Double, Int), b: (String, Double, Int)) =
      (a._1 ,a._2 + b._2, a._3 + b._3)
  }
}
