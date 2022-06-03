package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Random;

public class AllowLatenessExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据流有三个字段：（key, 时间戳, 数值）
        DataStream<Tuple3<String, Long, Integer>> input = env
                .addSource(new MySource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f1)
                );

        DataStream<Tuple4<String, String, Integer, String>> allowedLatenessStream = input
                .keyBy(item -> item.f0)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new AllowedLatenessFunction());

        allowedLatenessStream.print();

        env.execute("late elements");
    }

    /**
     * ProcessWindowFunction接收的泛型参数分别为：[输入类型、输出类型、Key、Window]
     */
    public static class AllowedLatenessFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, Integer, String>, String, TimeWindow> {
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple3<String, Long, Integer>> elements,
                            Collector<Tuple4<String, String, Integer, String>> out) throws Exception {
            ValueState<Boolean> isUpdated = context.windowState().getState(
                    new ValueStateDescriptor<Boolean>("isUpdated", Types.BOOLEAN));

            int count = 0;
            for (Object i : elements) {
                count++;
            }

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            if (null == isUpdated.value() || isUpdated.value() == false) {
                // 第一次使用process()函数时， Boolean默认初始化为false，因此窗口函数第一次被调用时会进入这里
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "first"));
                isUpdated.update(true);
            } else {
                // 之后isUpdated被置为true，窗口函数因迟到数据被调用时会进入这里
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "updated"));
            }
        }
    }

    public static class MySource implements SourceFunction<Tuple3<String, Long, Integer>>{

        private boolean isRunning = true;
        private Random rand = new Random();

        @Override
        public void run(SourceContext<Tuple3<String, Long, Integer>> sourceContext) throws Exception {
            while (isRunning) {
                long curTime = Calendar.getInstance().getTimeInMillis();
                // 增加一些延迟
                long eventTime = curTime + rand.nextInt(10000);

                // 将数据源收集写入SourceContext
                sourceContext.collect(Tuple3.of("1", eventTime, rand.nextInt()));
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
