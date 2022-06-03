package com.flink.tutorials.java.exercise.chapter7;

import com.flink.tutorials.java.exercise.wordcount.WordCountKafkaInKafkaOut;
import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * This demo shows how to calculate Volume-Weighted Average Price (VWAP)
 * of a stock data streams. The time window is 5 minutes.
 * See https://www.investopedia.com/terms/v/vwap.asp for VMAP
 *
 * To make it recoverable in case of failure, we should implement
 * a Checkpointed Source. We send processed data into Kafka.
 *
 * Make sure you have already started Kafka and created a Kafka topic
 * (in this case we use `caseStockVWAP` as Kafka topic) before running
 * this program.
 *
 * Kafka command line tips:
 *
 * Go to your Kafka installation folder:
 *     cd path/to/kafka
 *
 * Use the following command line to create the Kafka topic:
 *     bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
 * --replication-factor 1 --partitions 1 --topic stocks
 *
 * Use the following command line to check the result in Kafka topic:
 *     bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 *     --topic stocks --from-beginning
 */

public class StockPriceVWAPConnector {

    public static void main(String[] args) throws Exception {

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        String outputTopic = "stocks";

        // get Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getCheckpointConfig().setCheckpointInterval(2 * 1000);

        env.setParallelism(1);

        // read data stream
        DataStream<StockPrice> inputStream = env
                .addSource(new StockSourceCkpt("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        DataStream<Tuple2<String, Double>> stream = inputStream
                .keyBy(stockPrice -> stockPrice.symbol)
                .timeWindow(Time.seconds(5))
                .process(new VWAPProcessFunction());

//        stream.print();

        // Sink
        FlinkKafkaProducer<Tuple2<String, Double>> producer = new FlinkKafkaProducer<Tuple2<String, Double>> (
                outputTopic,
                new KafkaStockSerializationSchema(outputTopic),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        stream.addSink(producer);

        env.execute("stock price");
    }

    /**
     * Four Generic Types in the Function
     * IN: Input Data Type - StockPrice
     * OUT: Output Data Type - (String, Double, Double, Double, Double)
     * KEY：Key - String
     * W: Window - TimeWindow
     */
    public static class VWAPProcessFunction extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<StockPrice> elements, Collector<Tuple2<String ,Double>> out) {
            String symbol = key;
            Double volumeSum = 0.0d;
            Double priceVolumeSum = 0.0d;

            for (StockPrice element: elements) {
                priceVolumeSum += element.price * element.volume;
                volumeSum += element.volume;
            }

            out.collect(Tuple2.of(symbol, priceVolumeSum / volumeSum));
        }
    }

    public static class KafkaStockSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Double>> {

        private String topic;

        public KafkaStockSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Double> element, Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(topic, (element.f0 + ": " + element.f1).getBytes(StandardCharsets.UTF_8));

        }
    }
}
