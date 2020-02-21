package com.flink.tutorials.scala.utils.stock

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class MediaSource extends RichSourceFunction[Media]{

  var isRunning: Boolean = true
  // 起始timestamp 2020/1/8 9:30:0 与数据集中的起始时间相对应
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
