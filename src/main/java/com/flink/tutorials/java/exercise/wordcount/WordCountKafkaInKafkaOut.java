package com.flink.tutorials.java.exercise.wordcount;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class WordCountKafkaInKafkaOut {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        String inputTopic = "Shakespeare";
        String outputTopic = "WordCount";

        // Source
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), properties);
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
        FlinkKafkaProducer<Tuple2<String, Integer>> producer = new FlinkKafkaProducer<Tuple2<String, Integer>> (
                outputTopic,
                new KafkaWordCountSerializationSchema(outputTopic),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        wordCount.addSink(producer);

        // execute
        env.execute("kafka streaming word count");

    }

    public static class KafkaWordCountSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Integer>> {

        private String topic;

        public KafkaWordCountSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(topic, (element.f0 + ": " + element.f1).getBytes(StandardCharsets.UTF_8));
        }
    }
}
