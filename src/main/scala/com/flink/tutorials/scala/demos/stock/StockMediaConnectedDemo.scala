package com.flink.tutorials.scala.demos.stock

import java.util.Calendar

import StockPriceDemo.{StockPrice, StockPriceSource, StockPriceTimeAssigner}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

object StockMediaConnectedDemo {

  def main(args: Array[String]) {

    // 设置执行环境
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 每5秒生成一个Watermark
    env.getConfig.setAutoWatermarkInterval(5000L)

    // 股票价格数据流
    val stockPriceRawStream: DataStream[StockPrice] = env
      // 该数据流由StockPriceSource类随机生成
      .addSource(new StockPriceSource)
      // 设置 Timestamp 和 Watermark
      .assignTimestampsAndWatermarks(new StockPriceTimeAssigner)

    val mediaStatusStream: DataStream[Media] = env
      .addSource(new MediaSource)

    // 先将两个流connect，再进行keyBy
    val keyByConnect1: ConnectedStreams[StockPrice, Media] = stockPriceRawStream
      .connect(mediaStatusStream)
      .keyBy(0,0)

    // 先keyBy再connect
    val keyByConnect2: ConnectedStreams[StockPrice, Media] = stockPriceRawStream.keyBy(0)
      .connect(mediaStatusStream.keyBy(0))

    val alert1 = keyByConnect1.flatMap(new AlertFlatMap).print()

    val alerts2 = keyByConnect2.flatMap(new AlertFlatMap).print()

    // 执行程序
    env.execute("connect stock price with media status")
  }

  /** 媒体评价
    *
    * symbol 股票代号
    * timestamp 时间戳
    * status 评价 正面/一般/负面
    */
  case class Media(symbol: String, timestamp: Long, status: String)

  class MediaSource extends RichSourceFunction[Media]{

    var isRunning: Boolean = true

    val rand = new Random()
    var stockId = 0

    override def run(srcCtx: SourceContext[Media]): Unit = {

      while (isRunning) {
        // 每次从列表中随机选择一只股票
        stockId = rand.nextInt(5)

        var status: String = "NORMAL"
        if (rand.nextGaussian() > 0.9) {
          status = "POSITIVE"
        } else if (rand.nextGaussian() < 0.05) {
          status = "NEGATIVE"
        }

        val curTime = Calendar.getInstance.getTimeInMillis

        srcCtx.collect(Media(stockId.toString, curTime, status))

        Thread.sleep(rand.nextInt(100))
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

  case class Alert(symbol: String, timestamp: Long, alert: String)

  class AlertFlatMap extends RichCoFlatMapFunction[StockPrice, Media, Alert] {

    var priceMaxThreshold: List[Double] = List(101.0d, 201.0d, 301.0d, 401.0d, 501.0d)

    var mediaLevel: String = "NORMAL"

    override def flatMap1(stock: StockPrice, collector: Collector[Alert]) : Unit = {
      val stockId = stock.symbol.toInt
      if ("POSITIVE".equals(mediaLevel) && stock.price > priceMaxThreshold(stockId)) {
        collector.collect(Alert(stock.symbol, stock.timestamp, "POSITIVE"))
      }
    }

    override def flatMap2(media: Media, collector: Collector[Alert]): Unit = {
      mediaLevel = media.status
    }
  }

}
