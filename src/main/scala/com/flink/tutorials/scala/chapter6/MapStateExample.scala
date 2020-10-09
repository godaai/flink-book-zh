package com.flink.tutorials.scala.api.chapter6

import com.flink.tutorials.scala.utils.taobao.{UserBehavior, UserBehaviorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MapStateExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(8)

    // 获取数据源
    val sourceStream: DataStream[UserBehavior] = env
      .addSource(new UserBehaviorSource("taobao/UserBehavior-20171201.csv"))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
            override def extractTimestamp(t: UserBehavior, l: Long): Long = t.timestamp * 1000
          })
      )

    // 生成一个KeyedStream
    val keyedStream =  sourceStream.keyBy(user => user.userId)

    // 在KeyedStream上进行flatMap()
    val behaviorCountStream: DataStream[(Long, String, Int)] = keyedStream.flatMap(new MapStateFunction)

    behaviorCountStream.print()

    env.execute("taobao map state example")
  }

  class MapStateFunction extends RichFlatMapFunction[UserBehavior, (Long, String, Int)] {

    // 指向MapState的句柄
    private var behaviorMapState: MapState[String, Int] = _

    override def open(parameters: Configuration): Unit = {
      // 创建StateDescriptor
      val behaviorMapStateDescriptor = new MapStateDescriptor[String, Int]("behaviorMap", classOf[String], classOf[Int])
      // 通过StateDescriptor获取运行时上下文中的状态
      behaviorMapState = getRuntimeContext.getMapState(behaviorMapStateDescriptor)
    }

    override def flatMap(input: UserBehavior, collector: Collector[(Long, String, Int)]): Unit = {
      var behaviorCnt = 1
      // behavior有可能为pv、cart、fav、buy等
      // 判断状态中是否有该behavior
      if (behaviorMapState.contains(input.behavior)) {
        behaviorCnt = behaviorMapState.get(input.behavior) + 1
      }
      // 更新状态
      behaviorMapState.put(input.behavior, behaviorCnt)
      collector.collect((input.userId, input.behavior, behaviorCnt))
    }
  }
}
