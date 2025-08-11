package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class TriggerExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> source = FileSource
                .forRecordStreamFormat(new StockReaderFormat(), new Path(filePath))
                .build();
        DataStream<StockPrice> stockStream = env
                .fromSource(source, WatermarkStrategy
                                .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许5s乱序
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                        , "StockSource");

        // 打印输入数据
        stockStream.print("Stock Data");

        DataStream<Tuple2<String, Double>> average = stockStream
                .keyBy(s -> s.symbol)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(60)))
                .trigger(new MyTrigger())
                .aggregate(new AggregateFunctionExample.AverageAggregate());

        average.print();

        env.execute("trigger");
    }

    public static class MyTrigger extends Trigger<StockPrice, TimeWindow> {
        @Override
        public TriggerResult onElement(StockPrice element,
                                       long time,
                                       TimeWindow window,
                                       TriggerContext triggerContext) throws Exception {
            ValueState<Double> lastPriceState = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<>("lastPriceState", Types.DOUBLE)
            );

            // 设置返回默认值为CONTINUE
            TriggerResult triggerResult = TriggerResult.CONTINUE;

            // 第一次使用lastPriceState时状态是空的,需要先进行判断
            // 如果是空，返回一个null
            if (null != lastPriceState.value()) {
                double priceChange = lastPriceState.value() - element.price;
                double dropPercent = priceChange / lastPriceState.value();

                System.out.printf(
                        "[Trigger] Symbol: %s | Current Price: %.2f | Last Price: %.2f | Drop: %.2f%%%n",
                        element.symbol, element.price, lastPriceState.value(), dropPercent * 100
                );

                if (dropPercent > 0.05) {
                    // 如果价格跌幅大于5%，直接FIRE_AND_PURGE
                    System.out.println("[Trigger] 5% drop triggered!");
                    triggerResult = TriggerResult.FIRE_AND_PURGE;
                } else if (dropPercent > 0.01) {
                    // 跌幅不大，注册一个10秒后的Timer
                    long t = triggerContext.getCurrentProcessingTime() + 10 * 1000;
                    System.out.printf("[Trigger] 1%% drop, register timer @ %d%n", t);
                    triggerContext.registerProcessingTimeTimer(t);
                }
            } else {
                System.out.printf(
                        "[Trigger] First element for symbol: %s | Price: %.2f%n",
                        element.symbol, element.price
                );
            }
            lastPriceState.update(element.price);
            return triggerResult;
        }

        // 这里我们不用EventTime，直接返回一个CONTINUE
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext triggerContext) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext triggerContext) {
            System.out.printf("[Timer] Triggered at: %d%n", time);
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext triggerContext) {
            ValueState<Double> lastPriceState = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<>("lastPriceState", Types.DOUBLE)
            );
            lastPriceState.clear();
        }
    }
}