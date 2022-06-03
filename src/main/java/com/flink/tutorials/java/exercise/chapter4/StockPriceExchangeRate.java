package com.flink.tutorials.java.exercise.chapter4;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This demo shows how to get convert a data stream into another one by `map()`.
 * Suppose we want to get the price in another currency.
 * The exchange rate is 7.
 */

public class StockPriceExchangeRate {

    public static void main(String[] args) throws Exception {

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));

        DataStream<StockPrice> exchangeRateStream = stream
                .map(stockPrice ->
                        StockPrice.of(stockPrice.symbol, stockPrice.price * 7, stockPrice.ts, stockPrice.volume));

        exchangeRateStream.print();

        env.execute("stock price exchange rate");
    }
}
