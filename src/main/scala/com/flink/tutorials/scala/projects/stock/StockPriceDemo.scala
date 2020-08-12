package com.flink.tutorials.scala.projects.stock

import com.flink.tutorials.scala.utils.stock.{StockPrice, StockSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object StockPriceDemo {

  def main(args: Array[String]) {

    // 设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 每5秒生成一个Watermark
    env.getConfig.setAutoWatermarkInterval(5000L)

    // 股票价格数据流
    val stockPriceRawStream: DataStream[StockPrice] = env
      .addSource(new StockSource("stock/stock-tick-20200108.csv"))
      // 设置 Timestamp 和 Watermark
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[StockPrice] {
            override def extractTimestamp(t: StockPrice, l: Long): Long = t.ts
            }
          )
      )

    val stockPriceStream: DataStream[StockPrice] = stockPriceRawStream
      .keyBy(_.symbol)
      // 设置5秒的时间窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      // 取5秒内某一只股票的最大值
      .max("price")

    // 打印结果
    stockPriceStream.print()

    // 执行程序
    env.execute("Compute max stock price")
  }

}
