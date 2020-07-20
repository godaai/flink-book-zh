package com.flink.tutorials.scala.api.transformations

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object PartitionCustomExample {

  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 获取当前执行环境的默认并行度
    val defaultParalleism = senv.getParallelism

    // 设置所有算子的并行度为4，表示所有算子的并行执行的算子子任务数为4
    senv.setParallelism(4)

    val dataStream: DataStream[(Int, String)] = senv.fromElements((1, "123"), (2, "abc"), (3, "256"), (4, "zyx")
      , (5, "bcd"), (6, "666"))

    // 对(Int, String)中的第二个字段使用 MyPartitioner 中的重分布逻辑
    val partitioned: DataStream[(Int, String)] = dataStream.partitionCustom(new MyPartitioner, 1)

    partitioned.print()

    senv.execute("partition custom transformation")
  }

  /**
    * Partitioner[T] 其中泛型T为指定的字段类型
    * 重写partiton函数，并根据T字段对数据流中的所有元素进行数据重分配
    * */
  class MyPartitioner extends Partitioner[String] {

    val rand = scala.util.Random

    /**
      * key 泛型T 即根据哪个字段进行数据重分配，本例中是(Int, String)中的String
      * numPartitons 为当前有多少个并行实例
      * 函数返回值是一个Int 为该元素将被发送给下游第几个实例
      * */
    override def partition(key: String, numPartitions: Int): Int = {
      var randomNum = rand.nextInt(numPartitions / 2)

      // 如果字符串中包含数字，该元素将被路由到前半部分，否则将被路由到后半部分。
      if (key.exists(_.isDigit)) {
        return randomNum
      } else {
        return randomNum + numPartitions / 2
      }
    }
  }
}
