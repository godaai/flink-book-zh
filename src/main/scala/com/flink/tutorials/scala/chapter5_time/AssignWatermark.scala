package com.flink.tutorials.scala.api.chapter5

import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object AssignWatermark {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 每5000毫秒生成一个Watermark
    env.getConfig.setAutoWatermarkInterval(5000L)

    val socketSource = env.socketTextStream("localhost", 9000)

    val input: DataStream[(String, Long)] = socketSource.map{
      line => {
        val arr = line.split(" ")
        val id = arr(0)
        val time = arr(1).toLong
        (id, time)
      }
    }

    // 第二个字段是时间戳
    // 使用下面的方式，部分Intellij需要在设置中添加 -target:jvm-1.8
    // Preferences -> Build, Execution, Deployment -> Compiler -> Scala Compiler -> Default / Maven工程
    // Additional compiler options 行添加参数: -target:jvm-1.8
    val periodWatermark: DataStream[(String, Long)] = input
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator[(String, Long)](
            new WatermarkGeneratorSupplier[(String, Long)] {
              override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Long)] =
                new MyPeriodicGenerator
          }
          ).withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = t._2
          })
      )

    periodWatermark.print()

    env.execute("periodic and punctuated watermark")

  }

  // 定期生成Watermark
  // 数据流元素 (String, Long) 共两个字段
  // 第一个字段为数据本身
  // 第二个字段是时间戳
  class MyPeriodicGenerator extends WatermarkGenerator[(String, Long)] {
    final private val maxOutOfOrderness = 60 * 1000 // 1分钟

    private var currentMaxTimestamp = 0L // 已抽取的Timestamp最大值

    override def onEvent(event: (String, Long), eventTimestamp: Long, output: WatermarkOutput): Unit = {
      // 更新currentMaxTimestamp为当前遇到的最大值
      currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp)
    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      // Watermark比currentMaxTimestamp最大值慢1分钟
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness))
    }
  }

  // 逐个检查数据流中的元素，根据元素中的特殊字段，判断是否要生成Watermark
  // 数据流元素 (String, Long, Boolean) 共三个字段
  // 第一个字段为数据本身
  // 第二个字段是时间戳
  // 第三个字段判断是否为Watermark的标记
  class MyPunctuatedGenerator extends WatermarkGenerator[(String, Long, Boolean)] {
    override def onEvent(event: (String, Long, Boolean), eventTimestamp: Long, output: WatermarkOutput): Unit = {
      if (event._3) {
        output.emitWatermark(new Watermark(event._2))
      }
    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      // 这里不需要做任何事情，因为我们在 onEvent() 方法中生成了Watermark
    }
  }
}
