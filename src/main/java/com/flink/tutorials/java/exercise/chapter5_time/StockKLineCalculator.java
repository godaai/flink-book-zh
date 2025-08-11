package com.flink.tutorials.java.exercise.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.core.fs.Path;

import java.time.Duration;

/**
 * 股票 K 线数据计算
 * 计算每个 5 分钟窗口的：
 * 1. 开盘价（Open）
 * 2. 最高价（High）
 * 3. 最低价（Low）
 * 4. 收盘价（Close）
 * 5. 交易量加权平均价格（VWAP）
 */
public class StockKLineCalculator {
    public static void main(String[] args) throws Exception {
        // 配置 Flink 环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取股票数据文件
        String filePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> source = FileSource
                .forRecordStreamFormat(new StockReaderFormat(), new Path(filePath))
                .build();

        DataStream<StockPrice> stockStream = env.fromSource(source,
                WatermarkStrategy.<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.ts),
                "StockSource");

        // 计算 K 线数据
        DataStream<Tuple4<String, Double, Double, Double>> kLineData = stockStream
                .keyBy(value -> value.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5))) // 5分钟窗口
                .allowedLateness(Duration.ofMinutes(1))  // 允许 1 分钟的迟到数据
                .aggregate(new KLineAggregateFunction());

        // 计算 VWAP
        DataStream<Tuple3<String, Double, Double>> vwapData = stockStream
                .keyBy(value -> value.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5))) // 5分钟窗口
                .allowedLateness(Duration.ofMinutes(1))  // 允许 1 分钟的迟到数据
                .aggregate(new VWAPAggregateFunction());

        // 打印结果
        kLineData.print("K-Line Data");
        vwapData.print("VWAP Data");

        env.execute("Stock K-Line Calculator");
    }

    /**
     * K 线数据聚合函数
     */
    public static class KLineAggregateFunction implements AggregateFunction<StockPrice, KLineAccumulator, Tuple4<String, Double, Double, Double>> {
        @Override
        public KLineAccumulator createAccumulator() {
            return new KLineAccumulator();
        }

        @Override
        public KLineAccumulator add(StockPrice value, KLineAccumulator accumulator) {
            if (accumulator.open == null) {
                accumulator.open = value.price;
                accumulator.symbol = value.symbol;  // 记录 symbol
            }
            accumulator.high = Math.max(accumulator.high, value.price);
            accumulator.low = Math.min(accumulator.low, value.price);
            accumulator.close = value.price;
            return accumulator;
        }

        @Override
        public Tuple4<String, Double, Double, Double> getResult(KLineAccumulator accumulator) {
            return Tuple4.of(
                    accumulator.symbol,
                    accumulator.open,
                    accumulator.high,
                    accumulator.low
            );
        }

        @Override
        public KLineAccumulator merge(KLineAccumulator a, KLineAccumulator b) {
            // Ensure the symbol is carried over when merging
            if (a.symbol == null) {
                a.symbol = b.symbol;
            }
            return new KLineAccumulator(
                    a.symbol,
                    a.open == null ? b.open : a.open,
                    Math.max(a.high, b.high),
                    Math.min(a.low, b.low),
                    b.close
            );
        }
    }

    /**
     * VWAP 聚合函数
     */
    public static class VWAPAggregateFunction implements AggregateFunction<StockPrice, VWAPAccumulator, Tuple3<String, Double, Double>> {
        @Override
        public VWAPAccumulator createAccumulator() {
            return new VWAPAccumulator();
        }

        @Override
        public VWAPAccumulator add(StockPrice value, VWAPAccumulator accumulator) {
            if (accumulator.symbol == null) {
                accumulator.symbol = value.symbol;  // 记录 symbol
            }
            accumulator.totalPrice += value.price * value.volume;
            accumulator.totalVolume += value.volume;
            return accumulator;
        }

        @Override
        public Tuple3<String, Double, Double> getResult(VWAPAccumulator accumulator) {
            double vwap = accumulator.totalVolume > 0 ? accumulator.totalPrice / accumulator.totalVolume : 0;
            return Tuple3.of(
                    accumulator.symbol,
                    vwap,
                    accumulator.totalVolume
            );
        }

        @Override
        public VWAPAccumulator merge(VWAPAccumulator a, VWAPAccumulator b) {
            // Ensure the symbol is carried over when merging
            if (a.symbol == null) {
                a.symbol = b.symbol;
            }
            return new VWAPAccumulator(
                    a.symbol,
                    a.totalPrice + b.totalPrice,
                    a.totalVolume + b.totalVolume
            );
        }
    }

    /**
     * K 线数据累加器
     */
    public static class KLineAccumulator {
        public String symbol;
        public Double open;
        public Double high = Double.MIN_VALUE;  // Initial high value
        public Double low = Double.MAX_VALUE;   // Initial low value
        public Double close;

        public KLineAccumulator() {}

        public KLineAccumulator(String symbol, Double open, Double high, Double low, Double close) {
            this.symbol = symbol;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
        }
    }

    /**
     * VWAP 累加器
     */
    public static class VWAPAccumulator {
        public String symbol;
        public double totalPrice = 0;
        public double totalVolume = 0;

        public VWAPAccumulator() {}

        public VWAPAccumulator(String symbol, double totalPrice, double totalVolume) {
            this.symbol = symbol;
            this.totalPrice = totalPrice;
            this.totalVolume = totalVolume;
        }
    }
}
