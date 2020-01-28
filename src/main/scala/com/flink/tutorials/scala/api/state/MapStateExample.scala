package com.flink.tutorials.scala.api.state

import java.io.InputStream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MapStateExample {

  /**
    * 用户行为
    * categoryId为商品类目ID
    * behavior包括点击（pv）、购买（buy）、加购物车（cart）、喜欢（fav）
    * */
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

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

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(8)

    // 获取数据源
    val sourceStream: DataStream[UserBehavior] = env
      .addSource(new UserBehaviorSource("state/UserBehavior-50.csv")).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior]() {
          override def extractAscendingTimestamp(userBehavior: UserBehavior): Long = {
            // 原始数据单位为秒，乘以1000转换成毫秒
            userBehavior.timestamp * 1000
          }
        }
      )

    // 生成一个KeyedStream
    val keyedStream =  sourceStream.keyBy(user => user.userId)

    // 在KeyedStream上进行flatMap
    val behaviorCountStream = keyedStream.flatMap(new MapStateFunction)

    behaviorCountStream.print()

    env.execute("state example")
  }

  class UserBehaviorSource(path: String) extends RichSourceFunction[UserBehavior] {

    var isRunning: Boolean = true
    // 输入源
    var streamSource: InputStream = _

    override def run(sourceContext: SourceContext[UserBehavior]): Unit = {
      // 从项目的resources目录获取输入
      streamSource = MapStateExample.getClass.getClassLoader.getResourceAsStream(path)
      val lines: Iterator[String] = scala.io.Source.fromInputStream(streamSource).getLines
      while (isRunning && lines.hasNext) {
        val line = lines.next()
        val itemStrArr = line.split(",")
        val userBehavior = UserBehavior(itemStrArr(0).toLong, itemStrArr(1).toLong, itemStrArr(2).toInt, itemStrArr(3), itemStrArr(4).toLong)
        sourceContext.collect(userBehavior)
      }
    }

    override def cancel(): Unit = {
      streamSource.close()
      isRunning = false
    }
  }

}
