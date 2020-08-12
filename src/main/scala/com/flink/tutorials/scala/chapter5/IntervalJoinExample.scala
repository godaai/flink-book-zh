package com.flink.tutorials.scala.api.chapter5

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.getConfig.setAutoWatermarkInterval(2000L)

    val socketSource1 = senv.socketTextStream("localhost", 9000)
    val socketSource2 = senv.socketTextStream("localhost", 9001)

    // 数据流有三个字段：（key, 时间戳, 数值）
    val input1: DataStream[(String, Long, Int)] = socketSource1.flatMap {
      (line: String, out: Collector[(String, Long, Int)]) => {
        val array = line.split(" ")
        if (array.size == 3) {
          out.collect((array(0), array(1).toLong, array(2).toInt))
        }
      }
    }.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
          override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
        })
    )

    val input2: DataStream[(String, Long, Int)] = socketSource2.flatMap {
      (line: String, out: Collector[(String, Long, Int)]) => {
        val array = line.split(" ")
        if (array.size == 3) {
          out.collect((array(0), array(1).toLong, array(2).toInt))
        }
      }
    }.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })
    )

    val intervalJoinResult: DataStream[String] = input1.keyBy(_._1)
      .intervalJoin(input2.keyBy(_._1))
      .between(Time.milliseconds(-5), Time.milliseconds(10))
      .process(new MyProcessFunction)

    intervalJoinResult.print()

    senv.execute("interval join function")
  }

  class MyProcessFunction extends ProcessJoinFunction[(String, Long, Int), (String, Long, Int), String] {
    override def processElement(input1: (String, Long, Int),
                                input2: (String, Long, Int),
                                context: ProcessJoinFunction[(String, Long, Int), (String, Long, Int), String]#Context,
                                out: Collector[String]): Unit = {

      out.collect("input 1: " + input1.toString + ", input 2: " + input2.toString)

    }
  }

}
