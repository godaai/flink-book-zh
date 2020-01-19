package com.flink.tutorials.scala.api.time

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object AggregateFunctionExample {

  case class StockPrice(symbol: String, price: Double)

  // IN: StockPrice
  // ACC：(String, Double, Int) - (symbol, sum, count)
  // OUT: (String, Double) - (symbol, average)
  class AverageAggregate extends AggregateFunction[StockPrice, (String, Double, Int), (String, Double)] {

    override def createAccumulator() = ("", 0, 0)

    override def add(item: StockPrice, accumulator: (String, Double, Int)) =
      (item.symbol, accumulator._2 + item.price, accumulator._3 + 1)

    override def getResult(accumulator:(String, Double, Int)) = (accumulator._1 ,accumulator._2 / accumulator._3)

    override def merge(a: (String, Double, Int), b: (String, Double, Int)) =
      (a._1 ,a._2 + b._2, a._3 + b._3)
  }

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

    val average = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .aggregate(new AverageAggregate)

    average.print()

    senv.execute("window aggregate function")
  }

}
