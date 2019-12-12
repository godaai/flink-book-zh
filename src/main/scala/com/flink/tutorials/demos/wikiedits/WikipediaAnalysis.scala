package com.flink.tutorials.demos.wikiedits

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource
object WikipediaAnalysis {
  def main(args: Array[String]) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 需要外网环境
    val edits = env.addSource(new WikipediaEditsSource)

    val result = edits
      .map(line =>(line.isBotEdit,1))
      .keyBy(0)
      .timeWindow(Time.seconds(1))
      .sum(1)

    result.print()

    env.execute("Wikipedia Edit streaming")
  }
}
