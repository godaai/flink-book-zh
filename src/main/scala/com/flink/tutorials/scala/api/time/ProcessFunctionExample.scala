package com.flink.tutorials.scala.api.time

import java.io.InputStream
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionExample {

  case class StockPrice(symbol: String, ts: Long, price: Double, volume: Int)

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用EventTime时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    // 读入数据流
    val inputStream: DataStream[StockPrice] = env
      .addSource(new StockSource("time/us-stock-tick-20200108.csv"))
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[StockPrice]() {
        override def extractAscendingTimestamp(stock: StockPrice): Long = {
          stock.ts
        }
      }
    )

    val warnings = inputStream
      .keyBy(stock => stock.symbol)
      // 调用process函数
      .process(new IncreaseAlertFunction(10000))

    warnings.print()

    val outputTag: OutputTag[StockPrice] = OutputTag[StockPrice]("high-volume-trade")
    val sideOutputStream: DataStream[StockPrice] = warnings.getSideOutput(outputTag)

    sideOutputStream.print()

    env.execute("stock tick data")
  }

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

      out.collect("time: " + formatter.format(ts) + ", symbol: '" + ctx.getCurrentKey +
        " monotonically increased for " + intervalMills + " millisecond.")
      // 清空currentTimer状态
      currentTimer.clear()
    }
  }

  class StockSource(path: String) extends RichSourceFunction[StockPrice] {

    var isRunning: Boolean = true
    // 输入源
    var streamSource: InputStream = _

    override def run(sourceContext: SourceContext[StockPrice]): Unit = {
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss")
      // 从项目的resources目录获取输入
      streamSource = ProcessFunctionExample.getClass.getClassLoader.getResourceAsStream(path)
      val lines: Iterator[String] = scala.io.Source.fromInputStream(streamSource).getLines
      var isFirstLine: Boolean = true
      var timeDiff: Long = 0
      var lastEventTs: Long = 0
      while (isRunning && lines.hasNext) {
        val line = lines.next()
        val itemStrArr = line.split(",")
        val dateTime: LocalDateTime = LocalDateTime.parse(itemStrArr(1) + " " + itemStrArr(2), formatter)
        val eventTs: Long = Timestamp.valueOf(dateTime).getTime
        if (isFirstLine) {
          // 从第一行数据提取时间戳
          lastEventTs = eventTs
          isFirstLine = false
        }
        val stock = StockPrice(itemStrArr(0), eventTs, itemStrArr(3).toDouble, itemStrArr(4).toInt)
        // 输入文件中的时间戳是从小到大排列的
        // 新读入的行如果比上一行大，sleep，这样来模拟一个有时间间隔的输入流
        timeDiff = eventTs - lastEventTs
        if (timeDiff > 0)
          Thread.sleep(timeDiff)
        sourceContext.collect(stock)
        lastEventTs = eventTs
      }
    }

    override def cancel(): Unit = {
      streamSource.close()
      isRunning = false
    }
  }
}
