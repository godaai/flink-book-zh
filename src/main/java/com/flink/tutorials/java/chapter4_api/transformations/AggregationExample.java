package com.flink.tutorials.java.chapter4_api.transformations;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This class shows some aggregation functions (sum(), max() and maxBy()).
 * */

public class AggregationExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Integer, Integer, Integer>> tupleStream = senv.fromElements(
                Tuple3.of(0, 0, 0), Tuple3.of(0, 1, 1), Tuple3.of(0, 2, 2),
                Tuple3.of(1, 0, 6), Tuple3.of(1, 1, 7), Tuple3.of(1, 0, 8));

        // keyBy by the first field, sum the second field, output is as follow
        // 按第一个字段分组，对第二个字段求和，打印出来的结果如下：
        //  (0,0,0)
        //  (0,1,0)
        //  (0,3,0)
        //  (1,0,6)
        //  (1,1,6)
        //  (1,1,6)
        DataStream<Tuple3<Integer, Integer, Integer>> sumStream = tupleStream.keyBy(0).sum(1);
//        sumStream.print();

        // keyBy by the first field, get max of the third field with max(), output is as follow
        // 按第一个字段分组，对第三个字段求最大值，使用max()，打印出来的结果如下：
        //  (0,0,0)
        //  (0,0,1)
        //  (0,0,2)
        //  (1,0,6)
        //  (1,0,7)
        //  (1,0,8)
        DataStream<Tuple3<Integer, Integer, Integer>> maxStream = tupleStream.keyBy(0).max(2);
//        maxStream.print();

        // keyBy by the first field, get max of the third field with maxBy(), output is as follow
        // 按第一个字段分组，对第三个字段求最大值，使用maxBy()，打印出来的结果如下：
        //  (0,0,0)
        //  (0,1,1)
        //  (0,2,2)
        //  (1,0,6)
        //  (1,1,7)
        //  (1,0,8)
        DataStream<Tuple3<Integer, Integer, Integer>> maxByStream = tupleStream.keyBy(0).maxBy(2);
        maxByStream.print();

        senv.execute("basic aggregation transformation");

    }

}
