package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.StockPrice;
import com.flink.tutorials.java.utils.stock.StockSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        // 读入数据流
        DataStream<StockPrice> inputStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        DataStream<String> warnings = inputStream
                .keyBy(stock -> stock.symbol)
                // 调用process()函数
                .process(new IncreaseAlertFunction(3000));

        warnings.print();

        OutputTag<StockPrice> outputTag = new OutputTag<StockPrice>("high-volume-trade") {};

        SingleOutputStreamOperator<StockPrice> mainDataStream = (SingleOutputStreamOperator)inputStream;
        DataStream<StockPrice>sideOutputStream = mainDataStream.getSideOutput(outputTag);

//        sideOutputStream.print();

        env.execute("process function");
    }

    // 三个泛型分别为 Key、输入、输出
    public static class IncreaseAlertFunction
            extends KeyedProcessFunction<String, StockPrice, String> {

        private long intervalMills;
        // 状态句柄
        private ValueState<Double> lastPrice;
        private ValueState<Long> currentTimer;

        public IncreaseAlertFunction(long intervalMills) throws Exception {
            this.intervalMills = intervalMills;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从RuntimeContext中获取状态
            lastPrice = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("lastPrice", Types.DOUBLE));
            currentTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG));
        }

        @Override
        public void processElement(StockPrice stock, Context context, Collector<String> out) throws Exception {

            // 状态第一次使用时，未做初始化，返回null
            if (null == lastPrice.value()) {
                // 第一次使用lastPrice，不做任何处理
            } else {
                double prevPrice = lastPrice.value();
                long curTimerTimestamp;
                if (null == currentTimer.value()) {
                    curTimerTimestamp = 0;
                } else {
                    curTimerTimestamp = currentTimer.value();
                }
                if (stock.price < prevPrice) {
                    // 如果新流入的股票价格降低，删除Timer，否则该Timer一直保留
                    context.timerService().deleteEventTimeTimer(curTimerTimestamp);
                    currentTimer.clear();
                } else if (stock.price >= prevPrice && curTimerTimestamp == 0) {
                    // 如果新流入的股票价格升高
                    // curTimerTimestamp为0表示currentTimer状态中是空的，还没有对应的Timer
                    // 新Timer = 当前时间 + interval
                    long timerTs = context.timestamp() + intervalMills;

                    context.timerService().registerEventTimeTimer(timerTs);
                    // 更新currentTimer状态，后续数据会读取currentTimer，做相关判断
                    currentTimer.update(timerTs);
                }
            }
            // 更新lastPrice
            lastPrice.update(stock.price);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            out.collect(formatter.format(ts) + ", symbol: " + ctx.getCurrentKey() +
                    " monotonically increased for " + intervalMills + " millisecond.");
            // 清空currentTimer状态
            currentTimer.clear();
        }
    }
}
