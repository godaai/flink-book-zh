package com.flink.tutorials.scala.api.time

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object PeriodicWatermark {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 每5000毫秒生成一个Watermark
    env.getConfig.setAutoWatermarkInterval(5000L)

    val socketSource = env.socketTextStream("localhost", 9000)

    val input = socketSource.map{
      line => {
        val arr = line.split(" ")
        val id = arr(0)
        val time = arr(1).toLong
        (id, time)
      }
    }

    val watermark = input.assignTimestampsAndWatermarks(new MyPeriodicAssigner)
    val boundedOutOfOrder = input.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.minutes(1)) {
        override def extractTimestamp(element: (String, Long)): Long = {
          element._2
        }
    })

    env.execute("periodic and punctuated watermark")

  }

  // 假设数据流的元素有两个字段(String, Long)，其中第二个字段是该元素的时间戳
  class MyPeriodicAssigner extends AssignerWithPeriodicWatermarks[(String, Long)] {
    val bound: Long = 60 * 1000     // 1分钟
    var maxTs: Long = Long.MinValue // 已抽取的Timestamp最大值

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
      println("extractTimestamp is inovked for element " + element._1 + "@" + element._2)
      // 更新maxTs为当前遇到的最大值
      maxTs = maxTs.max(element._2)
      // 使用第二个字段作为这个元素的Event Time
      element._2
    }

    override def getCurrentWatermark: Watermark = {
      println("getCurrentWatermark method is invoked @:" + formatter.format(System.currentTimeMillis()))
      // Watermark比Timestamp最大值慢1分钟
      val watermark = new Watermark(maxTs - bound)
      println("watermark timestamp @:" + formatter.format(watermark.getTimestamp))
      watermark
    }
  }

  // 第二个字段是时间戳，第三个字段判断是否为Watermark的标记
  class MyPunctuatedAssigner extends AssignerWithPunctuatedWatermarks[(String, Long, Boolean)] {

    override def extractTimestamp(element: (String, Long, Boolean), previousElementTimestamp: Long): Long = {
      element._2
    }

    override def checkAndGetNextWatermark(element: (String, Long, Boolean), extractedTimestamp: Long): Watermark = {
      if (element._3)
        new Watermark(extractedTimestamp)
      else
        null
    }
  }

}
