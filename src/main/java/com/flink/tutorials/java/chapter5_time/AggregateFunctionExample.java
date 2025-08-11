package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class AggregateFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 由于 flink 默认在文件根目录, 与 src 目录同级 ,使用 ClassLoader 获取资源路径
        String resourcePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> source = FileSource
                .forRecordStreamFormat(new StockReaderFormat(), new Path(resourcePath))
                .build();

        DataStream<StockPrice> stockStream = env
                .fromSource(source, WatermarkStrategy
                        .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许5s乱序
                        .withTimestampAssigner((event, timestamp) -> event.ts), "StockSource");

        DataStream<Tuple2<String, Double>> average = stockStream
                .keyBy(s -> s.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new AverageAggregate());

        average.print();

        env.execute("window aggregate function");
    }

    /**
     * 接收三个泛型：
     * IN: StockPrice
     * ACC：(String, Double, Int) - (symbol, sum, count)
     * OUT: (String, Double) - (symbol, average)
     */
    public static class AverageAggregate implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return Tuple3.of("", 0d, 0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(StockPrice item, Tuple3<String, Double, Integer> accumulator) {
            if (item.price < 0) {
                System.err.println("Invalid price: " + item.price);
                return accumulator; // 忽略异常值
            }
            double price = accumulator.f1 + item.price;
            int count = accumulator.f2 + 1;
            return Tuple3.of(item.symbol, price, count);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
            double averagePrice = accumulator.f1 / accumulator.f2;
            double formattedPrice = Math.round(averagePrice * 100) / 100.0;
            return Tuple2.of(accumulator.f0, formattedPrice);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
