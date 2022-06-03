package com.flink.tutorials.java.chapter4_api.transformations;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class shows different way to re-partition:
 *    * lambda function
 *    * anonymous class
 *    * implement MapFunction class
 * */

public class PartitionCustomExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // get the default parallelism from StreamExecutionEnvironment
        // 获取当前执行环境的默认并行度
        int defaultParallelism = senv.getParallelism();

        // set parallelism to 4
        // 设置所有算子的并行度为4，表示所有算子的并行执行的算子子任务数为4
        senv.setParallelism(4);

        DataStream<Tuple2<Integer, String>> dataStream = senv.fromElements(
                Tuple2.of(1, "123"), Tuple2.of(2, "abc"),
                Tuple2.of(3, "256"), Tuple2.of(4, "zyx"),
                Tuple2.of(5, "bcd"), Tuple2.of(6, "666"));

        // re-partition the second field
        // 对(Integer, String)中的第二个字段String使用 MyPartitioner 中的重分布逻辑
        DataStream<Tuple2<Integer, String>> partitioned = dataStream.partitionCustom(new MyPartitioner(), 1);

        partitioned.print();

        senv.execute("partition custom transformation");
    }

    /**
     * Partitioner<T>
     *
     * Partitioner<T> 其中泛型T为指定的字段类型
     * 重写partiton函数，并根据T字段对数据流中的所有元素进行数据重分配
     * */
    public static class MyPartitioner implements Partitioner<String> {

        private Random rand = new Random();
        private Pattern pattern = Pattern.compile(".*\\d+.*");

        /**
         * key 泛型T 即根据哪个字段进行数据重分配，本例中是Tuple2(Integer, String)中的String
         * numPartitons 为当前有多少个并行实例
         * 函数返回值是一个int 该元素将被发送给下游第几个实例
         * */
        @Override
        public int partition(String key, int numPartitions) {
            int randomNum = rand.nextInt(numPartitions / 2);

            Matcher m = pattern.matcher(key);
            if (m.matches()) {
                return randomNum;
            } else {
                return randomNum + numPartitions / 2;
            }
        }
    }
}
