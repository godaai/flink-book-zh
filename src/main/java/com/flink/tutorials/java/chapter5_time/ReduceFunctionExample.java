package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class ReduceFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读入股票数据流
        String filePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> source = FileSource
                .forRecordStreamFormat(new StockReaderFormat(), new Path(filePath))
                .build();
        DataStream<StockPrice> stockStream = env
                .fromSource(source, WatermarkStrategy
                        .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许5s乱序
                        .withTimestampAssigner((event, timestamp) -> event.ts), "StockSource");

        // reduce的返回类型必须和输入类型StockPrice一致
        DataStream<StockPrice> sum = stockStream
                .keyBy(s -> s.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts, s1.volume + s2.volume));

        sum.print();

        env.execute("window reduce function");

    }
}
