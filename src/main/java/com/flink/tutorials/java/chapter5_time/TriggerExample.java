package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TriggerExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读入股票数据流
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-test.csv"));

        DataStream<Tuple2<String, Double>> average = stockStream
                .keyBy(s -> s.symbol)
                .timeWindow(Time.seconds(60))
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
                                       Trigger.TriggerContext triggerContext) throws Exception {
            ValueState<Double> lastPriceState = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Double>("lastPriceState", Types.DOUBLE)
            );

            // 设置返回默认值为CONTINUE
            TriggerResult triggerResult = TriggerResult.CONTINUE;

            // 第一次使用lastPriceState时状态是空的,需要先进行判断
            // 如果是空，返回一个null
            if (null != lastPriceState.value()) {
                if (lastPriceState.value() - element.price > lastPriceState.value() * 0.05) {
                    // 如果价格跌幅大于5%，直接FIRE_AND_PURGE
                    triggerResult = TriggerResult.FIRE_AND_PURGE;
                } else if ((lastPriceState.value() - element.price) > lastPriceState.value() * 0.01) {
                    // 跌幅不大，注册一个10秒后的Timer
                    long t = triggerContext.getCurrentProcessingTime() + (10 * 1000 - (triggerContext.getCurrentProcessingTime() % 10 * 1000));
                    triggerContext.registerProcessingTimeTimer(t);
                }
            }
            lastPriceState.update(element.price);
            return triggerResult;
        }

        // 这里我们不用EventTime，直接返回一个CONTINUE
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext triggerContext) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext triggerContext) {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, Trigger.TriggerContext triggerContext) {
            ValueState<Double> lastPriceState = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Double>("lastPriceState", Types.DOUBLE)
            );
            lastPriceState.clear();
        }
    }
}
