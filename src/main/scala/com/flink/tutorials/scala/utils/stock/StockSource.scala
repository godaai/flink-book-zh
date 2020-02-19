package com.flink.tutorials.scala.utils.stock

import java.io.InputStream
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class StockSource(path: String) extends RichSourceFunction[StockPrice] {

  var isRunning: Boolean = true
  // 输入源
  var streamSource: InputStream = _

  override def run(sourceContext: SourceContext[StockPrice]): Unit = {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss")
    // 从项目的resources目录获取输入
    streamSource = this.getClass.getClassLoader.getResourceAsStream(path)
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
      val stock = StockPrice(itemStrArr(0), itemStrArr(3).toDouble, eventTs, itemStrArr(4).toInt)
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
