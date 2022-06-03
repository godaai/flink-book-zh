package com.flink.tutorials.scala.api.chapter5

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object JoinExample {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val socketSource1 = senv.socketTextStream("localhost", 9000)
    val socketSource2 = senv.socketTextStream("localhost", 9001)

    val input1: DataStream[(String, Int)] = socketSource1.flatMap {
      (line: String, out: Collector[(String, Int)]) => {
        val array = line.split(" ")
        if (array.size == 2) {
          out.collect((array(0), array(1).toInt))
        }
      }
    }

    val input2: DataStream[(String, Int)] = socketSource2.flatMap {
      (line: String, out: Collector[(String, Int)]) => {
        val array = line.split(" ")
        if (array.size == 2) {
          out.collect((array(0), array(1).toInt))
        }
      }
    }


    val joinResult: DataStream[String] = input1.join(input2)
      .where(i1 => i1._1)
      .equalTo(i2 => i2._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .apply(new MyJoinFunction)

    joinResult.print()

    senv.execute("window join function")
  }

  class MyJoinFunction extends JoinFunction[(String, Int), (String, Int), String] {

    override def join(input1: (String, Int), input2: (String, Int)): String = {
      "input 1 :" + input1._2 + ", input 2 :" + input2._2
    }
  }
}
