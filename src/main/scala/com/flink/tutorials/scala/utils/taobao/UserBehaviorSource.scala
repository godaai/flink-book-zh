package com.flink.tutorials.scala.utils.taobao

import java.io.InputStream

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class UserBehaviorSource(path: String) extends RichSourceFunction[UserBehavior] {

  var isRunning: Boolean = true
  // 输入源
  var streamSource: InputStream = _

  override def run(sourceContext: SourceContext[UserBehavior]): Unit = {
    // 从项目的resources目录获取输入
    streamSource = this.getClass.getClassLoader.getResourceAsStream(path)
    val lines: Iterator[String] = scala.io.Source.fromInputStream(streamSource).getLines
    var isFirstLine: Boolean = true
    var timeDiff: Long = 0
    var lastEventTs: Long = 0
    while (isRunning && lines.hasNext) {
      val line = lines.next()
      val itemStrArr = line.split(",")
      val eventTs: Long = itemStrArr(4).toLong
      if (isFirstLine) {
        // 从第一行数据提取时间戳
        lastEventTs = eventTs
        isFirstLine = false
      }
      val userBehavior = UserBehavior(itemStrArr(0).toLong, itemStrArr(1).toLong, itemStrArr(2).toInt, itemStrArr(3), eventTs)
      // 输入文件中的时间戳是从小到大排列的
      // 新读入的行如果比上一行大，sleep，这样来模拟一个有时间间隔的输入流
      timeDiff = eventTs - lastEventTs
      if (timeDiff > 0)
        Thread.sleep(timeDiff * 1000)
      sourceContext.collect(userBehavior)
      lastEventTs = eventTs
    }
  }

  override def cancel(): Unit = {
    streamSource.close()
    isRunning = false
  }
}
