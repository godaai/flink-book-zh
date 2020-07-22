package com.flink.tutorials.java.projects.wordcount;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountKafkaInStdOut {

    public static void main(String[] args) throws Exception {

        // 创建Flink执行环境
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        // 访问 http://localhost:8082 可以看到Flink Web UI
//        conf.setInteger(RestOptions.PORT, 8082);
//        // 创建本地执行环境，并行度为2
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);

        // 是否开启算子链
        // env.disableOperatorChaining();

        // 设置整个作业的并行度
        // env.setParallelism(2);

        // Kafka参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        String inputTopic = "Shakespeare";

        // Source
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

        // Transformations
        // 使用Flink算子对输入流的文本进行操作
        // 按空格切词、计数、分区、设置时间窗口、聚合
        DataStream<Tuple2<String, Integer>> wordCount = stream
            .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
                String[] tokens = line.split("\\s");
                // 输出结果 (word, 1)
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<>(token, 1));
                    }
                }
            })
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(0)
            .timeWindow(Time.seconds(5))
            .sum(1);

        // Sink
        wordCount.print().setParallelism(1);

        // execute
        env.execute("kafka streaming word count");
    }
}
