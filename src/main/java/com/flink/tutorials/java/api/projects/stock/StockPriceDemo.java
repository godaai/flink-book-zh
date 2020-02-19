package com.flink.tutorials.java.api.projects.stock;

import com.flink.tutorials.java.api.utils.stock.StockPrice;
import com.flink.tutorials.java.api.utils.stock.StockSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockPriceDemo {

    public static void main(String[] args) throws Exception {

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-test.csv"));
        stream.print();

        env.execute("stock price");
    }
}
