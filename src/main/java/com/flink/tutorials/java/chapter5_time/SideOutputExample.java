package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputExample {

    private static OutputTag<StockPrice> highVolumeOutput = new OutputTag<StockPrice>("high-volume-trade"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<StockPrice> inputStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        SingleOutputStreamOperator<String> mainStream = inputStream
                .keyBy(stock -> stock.symbol)
                // 调用process()函数，包含侧输出逻辑
                .process(new SideOutputFunction());

        DataStream<StockPrice> sideOutputStream = mainStream.getSideOutput(highVolumeOutput);
        sideOutputStream.print();

        env.execute("side output");
    }

    public static class SideOutputFunction extends KeyedProcessFunction<String, StockPrice, String> {

        @Override
        public void processElement(StockPrice stock, Context context, Collector<String> out) throws Exception {

            if (stock.volume > 100) {
                context.output(highVolumeOutput, stock);
            } else {
                out.collect("normal tick data");
            }
        }
    }
}
