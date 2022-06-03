package com.flink.tutorials.java.exercise.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountSocketInStdOut {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Source
        DataStream<String> stream = env.socketTextStream("localhost", 9000);

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
        wordCount.print();

        // execute
        env.execute("kafka streaming word count");

    }
}
