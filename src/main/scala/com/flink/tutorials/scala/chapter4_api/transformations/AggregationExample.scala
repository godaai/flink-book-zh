package com.flink.tutorials.scala.api.transformations

import org.apache.flink.streaming.api.scala._

object AggregationExample {

  def main(args: Array[String]): Unit = {

    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tupleStream = senv.fromElements(
      (0, 0, 0), (0, 1, 1), (0, 2, 2),
      (1, 0, 6), (1, 1, 7), (1, 2, 8)
    )

    // 按第一个字段分组，对第二个字段求和，打印出来的结果如下：
    //  (0,0,0)
    //  (0,1,0)
    //  (0,3,0)
    //  (1,0,6)
    //  (1,1,6)
    //  (1,3,6)
    val sumStream = tupleStream.keyBy(0).sum(1)
    sumStream.print()

    // 按第一个字段分组，对第三个字段求最大值，使用max()，打印出来的结果如下：
    //  (0,0,0)
    //  (0,0,1)
    //  (0,0,2)
    //  (1,0,6)
    //  (1,0,7)
    //  (1,0,8)
    val maxStream = tupleStream.keyBy(0).max(2)
    maxStream.print()

    // 按第一个字段分组，对第三个字段求最大值，使用maxBy()，打印出来的结果如下：
    //  (0,0,0)
    //  (0,1,1)
    //  (0,2,2)
    //  (1,0,6)
    //  (1,1,7)
    //  (1,2,8)
    val maxByStream = tupleStream.keyBy(0).maxBy(2)
    maxByStream.print()

    senv.execute("basic aggregation transformation")

  }

}
