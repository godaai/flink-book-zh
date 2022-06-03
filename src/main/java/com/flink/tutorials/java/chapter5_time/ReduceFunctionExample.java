package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ReduceFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"));

        // reduce的返回类型必须和输入类型StockPrice一致
        DataStream<StockPrice> sum = stockStream
                .keyBy(s -> s.symbol)
                .timeWindow(Time.seconds(10))
                .reduce((s1, s2) -> StockPrice.of(s1.symbol, s2.price, s2.ts,s1.volume + s2.volume));

        sum.print();

        env.execute("window reduce function");

    }
}
