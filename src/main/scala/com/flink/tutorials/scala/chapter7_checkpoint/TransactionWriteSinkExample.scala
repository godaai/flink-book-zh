package com.flink.tutorials.scala.chapter7_checkpoint

import java.io.BufferedWriter
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeutils.base.{StringSerializer, VoidSerializer}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}

object TransactionWriteSinkExample {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    // 访问 http://localhost:8082 可以看到Flink Web UI
    conf.setInteger(RestOptions.PORT, 8082)
    // 创建本地执行环境，并行度为1
    val env = StreamExecutionEnvironment.createLocalEnvironment(1, conf)
    // 每隔5秒进行一次Checkpoint
    env.getCheckpointConfig.setCheckpointInterval(5 * 1000)
    val countStream = env.addSource(new CheckpointedSourceExample.CheckpointedSource)
    // 每隔一定时间模拟一次失败
    val result = countStream.map(new CheckpointedSourceExample.FailingMapper(20))
    // 类Unix系统的临时文件夹在/tmp下
    // Windows用户需要修改这个目录
    val preCommitPath = "/tmp/flink-sink-precommit"
    val commitedPath = "/tmp/flink-sink-commited"
    if (!Files.exists(Paths.get(preCommitPath))) Files.createDirectory(Paths.get(preCommitPath))
    if (!Files.exists(Paths.get(commitedPath))) Files.createDirectory(Paths.get(commitedPath))
    // 使用Exactly-Once语义的Sink，执行本程序时可以查看相应的输出目录，查看数据
    result.addSink(new TwoPhaseFileSink(preCommitPath, commitedPath))
    // 数据打印到屏幕上，无Exactly-Once保障，有数据重发现象
    result.print()
    env.execute("two file sink")
  }


  class TwoPhaseFileSink(var preCommitPath: String, var commitedPath: String) extends TwoPhaseCommitSinkFunction[(String, Int), String, Void](StringSerializer.INSTANCE, VoidSerializer.INSTANCE) {
    // 缓存
    var transactionWriter: BufferedWriter = _

    override def invoke(transaction: String, in: (String, Int), context: SinkFunction.Context): Unit = {
      transactionWriter.write(in._1 + " " + in._2 + "\n")
    }

    override def beginTransaction: String = {
      val time = LocalDateTime.now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
      val subTaskIdx = getRuntimeContext.getIndexOfThisSubtask
      val fileName = time + "-" + subTaskIdx
      val preCommitFilePath = Paths.get(preCommitPath + "/" + fileName)
      // 创建一个存储本次事务的文件
      Files.createFile(preCommitFilePath)
      transactionWriter = Files.newBufferedWriter(preCommitFilePath)
      System.out.println("Transaction File: " + preCommitFilePath)
      fileName
    }

    // 将当前数据由内存写入磁盘
    override def preCommit(transaction: String): Unit = {
      transactionWriter.flush()
      transactionWriter.close()
    }

    override def commit(transaction: String): Unit = {
      val preCommitFilePath = Paths.get(preCommitPath + "/" + transaction)
      if (Files.exists(preCommitFilePath)) {
        val commitedFilePath = Paths.get(commitedPath + "/" + transaction)
        try
          Files.move(preCommitFilePath, commitedFilePath)
        catch {
          case e: Exception =>
            System.out.println(e)
        }
      }
    }

    override def abort(transaction: String): Unit = {
      val preCommitFilePath = Paths.get(preCommitPath + "/" + transaction)
      // 如果中途遇到中断，将文件删除
      if (Files.exists(preCommitFilePath)) try
        Files.delete(preCommitFilePath)
      catch {
        case e: Exception =>
          System.out.println(e)
      }
    }
  }
}
