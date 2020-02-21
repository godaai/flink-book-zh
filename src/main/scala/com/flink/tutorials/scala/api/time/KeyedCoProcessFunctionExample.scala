package com.flink.tutorials.scala.api.time

import com.flink.tutorials.scala.utils.stock.{Media, MediaSource, StockPrice, StockSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object KeyedCoProcessFunctionExample {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用EventTime时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    // 读入股票数据流
    val stockStream: DataStream[StockPrice] = env
      .addSource(new StockSource("stock/stock-tick-20200108.csv"))
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[StockPrice]() {
        override def extractAscendingTimestamp(stock: StockPrice): Long = {
          stock.ts
        }
      })

    // 读入媒体评价数据流
    val mediaStream: DataStream[Media] = env
      .addSource(new MediaSource)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Media]() {
        override def extractAscendingTimestamp(media: Media): Long = {
          media.ts
        }
      })

    val joinStream: DataStream[StockPrice] = stockStream.connect(mediaStream)
      .keyBy(0, 0)
      // 调用process函数
      .process(new JoinStockMediaProcessFunction())

    joinStream.print()

    env.execute("stock tick data")
  }

  /**
    * 四个泛型
    * Key
    * 第一个流类型
    * 第二个流类型
    * 输出
    */
  class JoinStockMediaProcessFunction extends KeyedCoProcessFunction[String, StockPrice, Media, StockPrice] {

    // mediaState
    private var mediaState: ValueState[String] = _

    override def open(parameters: Configuration): Unit = {
      // 从RuntimeContext中获取状态
      mediaState = getRuntimeContext.getState(
        new ValueStateDescriptor[String]("mediaStatusState", classOf[String]))

    }

    override def processElement1(stock: StockPrice,
                                 context: KeyedCoProcessFunction[String, StockPrice, Media, StockPrice]#Context,
                                 collector: Collector[StockPrice]): Unit = {
      val mediaStatus = mediaState.value()
      if (null != mediaStatus) {
        val newStock = stock.copy(mediaStatus = mediaStatus)
        collector.collect(newStock)
      }
    }

    override def processElement2(media: Media,
                                 context: KeyedCoProcessFunction[String, StockPrice, Media, StockPrice]#Context,
                                 collector: Collector[StockPrice]): Unit = {
      // 第二个流更新mediaState
      mediaState.update(media.status)
    }
  }
}
