package com.flink.tutorials.java.exercise.chapter4;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This demo shows how to get the max price of a stock data stream.
 */

public class StockPriceDemo {

    public static void main(String[] args) throws Exception {

        // get Execution Environment
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
