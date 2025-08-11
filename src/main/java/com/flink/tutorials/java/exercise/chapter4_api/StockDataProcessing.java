/*
 * 目前是批处理，暂时没想到怎么通过文件读取来模拟流处理
 */
package com.flink.tutorials.java.exercise.chapter4_api;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.Path;

import java.time.Duration;


public class StockDataProcessing {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        String filePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> source = FileSource
        .forRecordStreamFormat(new StockReaderFormat(), new Path(filePath))
        .build();
        //使用file source读取股票数据文件
        DataStream<StockPrice> stockStream = env.fromSource(source, WatermarkStrategy.
        <StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> event.ts), "StockSource");

        // 任务1 实时计算某只股票的最大值
        stockStream.keyBy(value -> value.symbol)
           .maxBy("price")
           .print();

        // 任务2 汇率转换
        stockStream.map(value -> new StockPrice(value.symbol, value.price * 7, value.ts, value.volume, value.mediaStatus))
           .print();

        // 任务3 大额交易过滤
        stockStream.filter(value -> value.volume > 1000)
            .print();

        env.execute("Stock Data Processing");
    }
}
