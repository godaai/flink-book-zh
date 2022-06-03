package com.flink.tutorials.scala.chapter7_checkpoint

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

object CheckpointedSourceExample {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    // 访问 http://localhost:8082 可以看到Flink Web UI
    conf.setInteger(RestOptions.PORT, 8082)
    // 创建本地执行环境，并行度为1
    val env = StreamExecutionEnvironment.createLocalEnvironment(1, conf)
    // 每隔2秒进行一次Checkpoint
    env.getCheckpointConfig.setCheckpointInterval(2 * 1000)
    val countStream = env.addSource(new CheckpointedSourceExample.CheckpointedSource)
    // 每隔一定时间模拟一次失败
    val result = countStream.map(new CheckpointedSourceExample.FailingMapper(20))
    result.print()
    env.execute("checkpointed source")
  }

  class CheckpointedSource extends RichSourceFunction[(String, Int)] with CheckpointedFunction {
    private var offset = 0
    private var isRunning = true
    private var offsetState: ListState[Int] = _

    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      while ( {
        isRunning
      }) {
        Thread.sleep(100)
        ctx.getCheckpointLock.synchronized {
          ctx.collect(new (String, Int)("" + offset, 1))
          offset += 1
        }

        if (offset == 1000)
          isRunning = false
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }

    override def snapshotState(snapshotContext: FunctionSnapshotContext): Unit = { // 清除上次状态
      offsetState.clear()
      // 将最新的offset添加到状态中
      offsetState.add(offset)
    }

    override def initializeState(initializationContext: FunctionInitializationContext): Unit = { // 初始化offsetState
      val desc = new ListStateDescriptor[Int]("offset", classOf[Int])
      offsetState = initializationContext.getOperatorStateStore.getListState(desc)
      val iter = offsetState.get
      if (iter == null || !iter.iterator.hasNext) { // 第一次初始化，从0开始计数
        offset = 0
      }
      else { // 从状态中恢复offset
        offset = iter.iterator.next
      }
    }
  }

  class FailingMapper(var failInterval: Int) extends MapFunction[(String, Int), (String, Int)] {
    private var count = 0

    override def map(in: (String, Int)): (String, Int) = {
      count += 1
      if (count > failInterval) throw new RuntimeException("job fail! show how flink checkpoint and recovery")
      in
    }
  }

}
