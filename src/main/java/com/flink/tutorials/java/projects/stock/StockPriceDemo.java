package com.flink.tutorials.java.projects.stock;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockPriceDemo {

    public static void main(String[] args) throws Exception {

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<StockPrice> maxStream = stream
                .keyBy("symbol")
                .max("price");

        maxStream.print();

        env.execute("stock price");
    }
}
