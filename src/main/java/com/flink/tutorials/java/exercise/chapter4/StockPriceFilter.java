package com.flink.tutorials.java.exercise.chapter4;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This demo shows how to do `filter()` on a stock data stream.
 * We want to filter elements with volume bigger than 200.
 */

public class StockPriceFilter {

    public static void main(String[] args) throws Exception {

        // get Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Float volumnThreshold = 200.0f;

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<StockPrice> largeVolumnStream = stream
                .filter(stockPrice -> stockPrice.volume > volumnThreshold);

        largeVolumnStream.print();

        env.execute("stock price filter");
    }
}
