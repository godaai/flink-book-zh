package com.flink.tutorials.scala.api.chapter5

import java.text.SimpleDateFormat

import com.flink.tutorials.scala.utils.stock.{StockPrice, StockSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionExample {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用EventTime时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    // 读入数据流
    val inputStream: DataStream[StockPrice] = env
      .addSource(new StockSource("stock/stock-tick-20200108.csv"))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[StockPrice] {
            override def extractTimestamp(t: StockPrice, l: Long): Long = t.ts
          })
      )

    val warnings: DataStream[String] = inputStream
      .keyBy(stock => stock.symbol)
      // 调用process()函数
      .process(new IncreaseAlertFunction(10000))

    warnings.print()

    val outputTag: OutputTag[StockPrice] = OutputTag[StockPrice]("high-volume-trade")
    val sideOutputStream: DataStream[StockPrice] = warnings.getSideOutput(outputTag)

    sideOutputStream.print()

    env.execute("stock tick data")
  }

  // 三个泛型分别为 Key、输入、输出
  class IncreaseAlertFunction(intervalMills: Long)
    extends KeyedProcessFunction[String, StockPrice, String] {

    // 状态句柄
    private var lastPrice: ValueState[Double] = _
    private var currentTimer: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {

      // 从RuntimeContext中获取状态
      lastPrice = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastPrice", classOf[Double]))
      currentTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))
    }

    override def processElement(stock: StockPrice,
                                context: KeyedProcessFunction[String, StockPrice, String]#Context,
                                out: Collector[String]): Unit = {

      // 获取lastPrice状态中的数据，第一次使用时会被初始化为0
      val prevPrice = lastPrice.value()
      // 更新lastPrice
      lastPrice.update(stock.price)
      val curTimerTimestamp = currentTimer.value()
      if (prevPrice == 0.0) {
        // 第一次使用，不做任何处理
      } else if (stock.price < prevPrice) {
        // 如果新流入的股票价格降低，删除Timer，否则该Timer一直保留
        context.timerService().deleteEventTimeTimer(curTimerTimestamp)
        currentTimer.clear()
      } else if (stock.price >= prevPrice && curTimerTimestamp == 0) {
        // 如果新流入的股票价格升高
        // curTimerTimestamp为0表示currentTimer状态中是空的，还没有对应的Timer
        // 新Timer = 当前时间 + interval
        val timerTs = context.timestamp() + intervalMills

        context.timerService().registerEventTimeTimer(timerTs)
        // 更新currentTimer状态，后续数据会读取currentTimer，做相关判断
        currentTimer.update(timerTs)
      }

      val highVolumeOutput: OutputTag[StockPrice] = new OutputTag[StockPrice]("high-volume-trade")

      if (stock.volume > 1000) {
        context.output(highVolumeOutput, stock)
      }
    }

    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, StockPrice, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      out.collect(formatter.format(ts) + ", symbol: " + ctx.getCurrentKey +
        " monotonically increased for " + intervalMills + " millisecond.")
      // 清空currentTimer状态
      currentTimer.clear()
    }
  }
}
