package com.flink.tutorials.scala.api.state

import java.io.InputStream

import org.apache.flink.api.common.state.{BroadcastState, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastStateExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(8)

    // 获取数据源
    val userBehaviorStream: DataStream[UserBehavior] = env
      .addSource(new UserBehaviorSource("state/UserBehavior-20171201.csv")).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior]() {
      override def extractAscendingTimestamp(userBehavior: UserBehavior): Long = {
        // 原始数据单位为秒，乘以1000转换成毫秒
        userBehavior.timestamp * 1000
      }
    })

    // BehaviorPattern数据流
    val patternStream: DataStream[BehaviorPattern] = env.fromElements(BehaviorPattern("pv", "buy"))

    // Broadcast State只能使用 Key->Value 结构，基于MapStateDescriptor
    val broadcastStateDescriptor =
      new MapStateDescriptor[Void, BehaviorPattern]("behaviorPattern", classOf[Void], classOf[BehaviorPattern])
    val broadcastStream: BroadcastStream[BehaviorPattern] = patternStream
      .broadcast(broadcastStateDescriptor)

    // 生成一个KeyedStream
    val keyedStream =  userBehaviorStream.keyBy(user => user.userId)
    // 在KeyedStream上进行connect和process
    val matchedStream = keyedStream
      .connect(broadcastStream)
      .process(new BroadcastPatternFunction)

    matchedStream.print()

    env.execute("broadcast state example")
  }

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

  /**
    * 行为模式
    * 整个模式简化为两个行为
    * */
  case class BehaviorPattern(firstBehavior: String, secondBehavior: String)

  /**
    * 四个泛型分别为：
    * 1. KeyedStream中Key的数据类型
    * 2. 主数据流的数据类型
    * 3. 广播流的数据类型
    * 4. 输出类型
    * */
  class BroadcastPatternFunction
    extends KeyedBroadcastProcessFunction[Long, UserBehavior, BehaviorPattern, (Long, BehaviorPattern)] {

    // 用户上次性能状态句柄，每个用户存储一个状态
    private var lastBehaviorState: ValueState[String] = _
    // Broadcast State Descriptor
    private var bcPatternDesc: MapStateDescriptor[Void, BehaviorPattern] = _

    override def open(parameters: Configuration): Unit = {

      lastBehaviorState = getRuntimeContext.getState(
        new ValueStateDescriptor[String]("lastBehaviorState", classOf[String])
      )

      bcPatternDesc = new MapStateDescriptor[Void, BehaviorPattern]("behaviorPattern", classOf[Void], classOf[BehaviorPattern])

    }

    // 当BehaviorPattern流有新数据时，更新BroadcastState
    override def processBroadcastElement(pattern: BehaviorPattern,
                                         context: KeyedBroadcastProcessFunction[Long, UserBehavior, BehaviorPattern, (Long, BehaviorPattern)]#Context,
                                         collector: Collector[(Long, BehaviorPattern)]): Unit = {

      val bcPatternState: BroadcastState[Void, BehaviorPattern] = context.getBroadcastState(bcPatternDesc)
      // 将新数据更新至Broadcast State，这里使用一个null作为Key
      // 在本场景中所有数据都共享一个Pattern，因此这里伪造了一个Key
      bcPatternState.put(null, pattern)
    }

    override def processElement(userBehavior: UserBehavior,
                                context: KeyedBroadcastProcessFunction[Long, UserBehavior, BehaviorPattern, (Long, BehaviorPattern)]#ReadOnlyContext,
                                collector: Collector[(Long, BehaviorPattern)]): Unit = {

      // 获取最新的Broadcast State
      val pattern: BehaviorPattern = context.getBroadcastState(bcPatternDesc).get(null)
      val lastBehavior: String = lastBehaviorState.value()
      if (pattern != null && lastBehavior != null) {
        // 用户之前有过行为，检查是否符合给定的模式
        if (pattern.firstBehavior.equals(lastBehavior) &&
        pattern.secondBehavior.equals(userBehavior.behavior))
          // 当前用户行为符合模式
        collector.collect((userBehavior.userId, pattern))
      }
      lastBehaviorState.update(userBehavior.behavior)
    }
  }

  class UserBehaviorSource(path: String) extends RichSourceFunction[UserBehavior] {

    var isRunning: Boolean = true
    // 输入源
    var streamSource: InputStream = _

    override def run(sourceContext: SourceContext[UserBehavior]): Unit = {
      // 从项目的resources目录获取输入
      streamSource = BroadcastStateExample.getClass.getClassLoader.getResourceAsStream(path)
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

}
