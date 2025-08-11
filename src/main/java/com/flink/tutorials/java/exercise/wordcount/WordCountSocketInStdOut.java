package com.flink.tutorials.java.exercise.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountSocketInStdOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> text = env.socketTextStream("localhost", 9000);

        DataStream<Tuple2<String, Integer>> counts = text
            .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                String[] words = line.split("[\\W]+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        out.collect(new Tuple2<>(word.toLowerCase(), 1));
                    }
                }
            })
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(value -> value.f0)
            .sum(1);

        counts.print();
        env.execute("Socket WordCount");
    }
}
