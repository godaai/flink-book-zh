package com.flink.tutorials.scala.api.chapter5

import com.flink.tutorials.scala.utils.stock.{StockPrice, StockSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object ProcessWindowFunctionExample {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val input = senv.addSource(new StockSource("stock/stock-tick-20200108.csv"))

    val frequency = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .process(new FrequencyProcessFunction)

    frequency.print()

    senv.execute("window process function")
  }

  /**
    * 接收四个泛型：
    * IN: 输入类型 StockPrice
    * OUT: 输出类型 (String, Double)
    * KEY：Key String
    * W: 窗口 TimeWindow
    */
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
      if (sortedMap.size > 0) out.collect((key, sortedMap(0)._1))
    }
  }
}
