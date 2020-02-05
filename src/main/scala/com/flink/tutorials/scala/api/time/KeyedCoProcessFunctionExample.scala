package com.flink.tutorials.scala.api.time

import java.io.InputStream
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

object KeyedCoProcessFunctionExample {

  case class StockPrice(symbol: String, ts: Long, price: Double, volume: Int, mediaStatus: String)

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用EventTime时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    // 读入数据流
    val stockStream: DataStream[StockPrice] = env
      .addSource(new StockSource("time/us-stock-tick-20200108.csv"))
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[StockPrice]() {
        override def extractAscendingTimestamp(stock: StockPrice): Long = {
          stock.ts
        }
      })

    val mediaStream: DataStream[Media] = env
      .addSource(new MediaSource)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Media]() {
        override def extractAscendingTimestamp(media: Media): Long = {
          media.ts
        }
      })

    val joinStream = stockStream.connect(mediaStream)
      .keyBy(0, 0)
      // 调用process函数
      .process(new JoinStockMediaProcessFunction())

    joinStream.print()

    env.execute("stock tick data")
  }

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
        val stock = StockPrice(itemStrArr(0), eventTs, itemStrArr(3).toDouble, itemStrArr(4).toInt, "")
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

  case class Media(symbol: String, ts: Long, status: String)

  class MediaSource extends RichSourceFunction[Media]{

    var isRunning: Boolean = true
    val startTs = 1578447000000L

    val rand = new Random()
    val symbolList = List("US2.AAPL", "US1.AMZN", "US1.BABA")

    override def run(srcCtx: SourceContext[Media]): Unit = {

      var inc = 0
      while (isRunning) {

        for (symbol <- symbolList) {
          // 给每支股票随机生成一个评价
          var status: String = "NORMAL"
          if (rand.nextGaussian() > 0.05) {
            status = "POSITIVE"
          }
          srcCtx.collect(Media(symbol, startTs + inc * 1000, status))
        }
        inc += 1
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

}
