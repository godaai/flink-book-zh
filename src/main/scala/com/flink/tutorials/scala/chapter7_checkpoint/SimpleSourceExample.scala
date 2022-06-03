package com.flink.tutorials.scala.chapter7_checkpoint

import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.source.SourceFunction

object SimpleSourceExample {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    // 访问 http://localhost:8082 可以看到Flink Web UI
    conf.setInteger(RestOptions.PORT, 8082)
    // 创建本地执行环境，并行度为2
    val env = StreamExecutionEnvironment.createLocalEnvironment(2, conf)
    val countStream = env.addSource(new SimpleSource)
    System.out.println("parallelism: " + env.getParallelism)

    countStream.print()
    env.execute("source")
  }

  class SimpleSource extends SourceFunction[(String, Integer)] {
    private var offset = 0
    private var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[(String, Integer)]): Unit = {
      while ( {
        isRunning
      }) {
        Thread.sleep(500)
        ctx.collect(new (String, Integer)("" + offset, offset))
        offset += 1
        if (offset == 1000)
          isRunning = false
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

}
