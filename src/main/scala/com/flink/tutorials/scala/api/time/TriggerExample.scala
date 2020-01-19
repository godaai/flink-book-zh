package com.flink.tutorials.scala.api.time

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {

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

  class MyTrigger extends Trigger[StockPrice, TimeWindow] {

    override def onElement(element: StockPrice,
                           time: Long,
                           window: TimeWindow,
                           triggerContext: Trigger.TriggerContext): TriggerResult = {
      val lastPriceState: ValueState[Double] = triggerContext.getPartitionedState(new ValueStateDescriptor[Double]("lastPriceState", classOf[Double]))

      // 设置返回默认值为CONTINUE
      var triggerResult: TriggerResult = TriggerResult.CONTINUE

      // 第一次使用lastPriceState时状态是空的,需要先进行判断
      // 状态数据由Java端生成，如果是空，返回一个null
      // 如果直接使用Scala的Double，需要使用下面的方法判断是否为空
      if (Option(lastPriceState.value()).isDefined) {
        if ((lastPriceState.value() - element.price) > lastPriceState.value() * 0.05) {
          // 如果价格跌幅大于5%，直接FIRE_AND_PURGE
          triggerResult = TriggerResult.FIRE_AND_PURGE
        } else if ((lastPriceState.value() - element.price) > lastPriceState.value() * 0.01) {
          val t = triggerContext.getCurrentProcessingTime + (10 * 1000 - (triggerContext.getCurrentProcessingTime % 10 * 1000))
          // 给10秒后注册一个Timer
          triggerContext.registerProcessingTimeTimer(t)
        }
      }
      lastPriceState.update(element.price)
      triggerResult
    }

    // 我们不用EventTime，直接返回一个CONTINUE
    override def onEventTime(time: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def clear(window: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
      val lastPrice: ValueState[Double] = triggerContext.getPartitionedState(new ValueStateDescriptor[Double]("lastPrice", classOf[Double]))
      lastPrice.clear()
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

    val average = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(60))
      .trigger(new MyTrigger)
      .aggregate(new AverageAggregate)

    average.print()

    senv.execute("trigger")
  }

}
