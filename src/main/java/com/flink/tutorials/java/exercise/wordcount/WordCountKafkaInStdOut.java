package com.flink.tutorials.java.exercise.wordcount;

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

         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration conf = new Configuration();
//        // For Flink Web UI, open the link in your broswer http://localhost:8082
//        conf.setInteger(RestOptions.PORT, 8082);
//        // create local StreamExecutionEnvironment with parallelism setted as 2
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);

        // enable or disable operator chaining
        // env.disableOperatorChaining();

        // parallelism setted as 2
        // env.setParallelism(2);

        // Kafka properties
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
        // use Flink API to process text stream
        // split text line into tokens by white space
        // count by token
        // apply time window and sum
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
