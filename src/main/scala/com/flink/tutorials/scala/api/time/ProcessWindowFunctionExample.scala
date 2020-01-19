package com.flink.tutorials.scala.api.time

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object ProcessWindowFunctionExample {

  case class StockPrice(symbol: String, price: Double)

  // ProcessWindowFunction接收的泛型参数分别为：[输入类型、输出类型、Key、Window]
  class FrequencyProcessFunction extends ProcessWindowFunction[StockPrice, (String, Double), String, TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[StockPrice], out: Collector[(String, Double)]): Unit = {

      // 股票价格和该价格出现的次数
      var countMap = scala.collection.mutable.Map[Double, Int]()

      for(element <- elements) {
        val count = countMap.getOrElse(element.price, 0)
        countMap(element.price) = count + 1
      }

      // 按照出现次数从高到低排序
      val sortedMap = countMap.toSeq.sortWith(_._2 > _._2)

      // 选出出现次数最高的输出到Collector
      if (sortedMap.size > 0) {
        out.collect((key, sortedMap(0)._1))
      }

    }
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

    val frequency = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .process(new FrequencyProcessFunction)

    frequency.print()

    senv.execute("window process function")
  }

}
