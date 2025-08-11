//package com.flink.tutorials.java.exercise.wordcount;
//
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.util.Collector;
//import java.util.Properties;
//
//public class WordCountKafkaInKafkaOut {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
//
//        Properties inputProps = new Properties();
//        inputProps.setProperty("bootstrap.servers", "localhost:9092");
//        inputProps.setProperty("group.id", "wordcount-group");
//
//        Properties outputProps = new Properties();
//        outputProps.setProperty("bootstrap.servers", "localhost:9092");
//
//        DataStream<String> text = env
//            .addSource(new FlinkKafkaConsumer<>(
//                "input-topic",
//                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
//                inputProps));
//
//        DataStream<String> counts = text
//            .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
//                String[] words = line.split("[\\W]+");
//                for (String word : words) {
//                    if (!word.isEmpty()) {
//                        out.collect(new Tuple2<>(word.toLowerCase(), 1));
//                    }
//                }
//            })
//            .returns(Types.TUPLE(Types.STRING, Types.INT))
//            .keyBy(value -> value.f0)
//            .sum(1)
//            .map(tuple -> tuple.f0 + ":" + tuple.f1);
//
//        counts.addSink(new FlinkKafkaProducer<>(
//            "output-topic",
//            new org.apache.flink.api.common.serialization.SimpleStringSchema(),
//            outputProps
//        ));
//
//        env.execute("Kafka WordCount Kafka Out");
//    }
//}
