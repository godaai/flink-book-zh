package com.flink.tutorials.demos.stock

import java.util.Calendar

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}

import scala.util.Random

object StockPriceDemo {

  /**
    * Case Class StockPrice
    * symbol 股票代号
    * timestamp 时间戳
    * price 价格
    */
  case class StockPrice(symbol: String, timestamp: Long, price: Double)

  def main(args: Array[String]) {

    // 设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 每5秒生成一个Watermark
    env.getConfig.setAutoWatermarkInterval(5000L)

    // 股票价格数据流
    val stockPriceRawStream: DataStream[StockPrice] = env
      // 该数据流由StockPriceSource类随机生成
      .addSource(new StockPriceSource)
      // 设置 Timestamp 和 Watermark
      .assignTimestampsAndWatermarks(new StockPriceTimeAssigner)

    val stockPriceStream: DataStream[StockPrice] = stockPriceRawStream
      .keyBy(_.symbol)
      // 设置5秒的时间窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      // 取5秒内某一只股票的最大值
      .max("price")

    // 打印结果
    stockPriceStream.print()

    // 执行程序
    env.execute("Compute max stock price")
  }

  class StockPriceSource extends RichSourceFunction[StockPrice]{

    var isRunning: Boolean = true

    val rand = new Random()
    // 初始化股票价格
    var priceList: List[Double] = List(100.0d, 200.0d, 300.0d, 400.0d, 500.0d)
    var stockId = 0
    var curPrice = 0.0d

    override def run(srcCtx: SourceContext[StockPrice]): Unit = {

      while (isRunning) {
        // 每次从列表中随机选择一只股票
        stockId = rand.nextInt(priceList.size)

        val curPrice =  priceList(stockId) + rand.nextGaussian() * 0.05
        priceList = priceList.updated(stockId, curPrice)
        val curTime = Calendar.getInstance.getTimeInMillis

        // 将数据源收集写入SourceContext
        srcCtx.collect(StockPrice("symbol_" + stockId.toString, curTime, curPrice))
        Thread.sleep(rand.nextInt(10))
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

  class StockPriceTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[StockPrice](Time.seconds(5)) {
    override def extractTimestamp(t: StockPrice): Long = t.timestamp
  }

}
