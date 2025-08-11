package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class AllowLatenessExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取当前系统时间
        long baseTime = System.currentTimeMillis();  // 当前时间戳

        // 生成测试数据
        List<Tuple3<String, Long, Integer>> inputData = new ArrayList<>();

        // 生成一些正常数据和延迟数据
        // 1. 第一个窗口的数据（0-5秒）
        inputData.add(Tuple3.of("1", baseTime + 1000, 1));        // 1秒
        inputData.add(Tuple3.of("1", baseTime + 2000, 2));        // 2秒
        inputData.add(Tuple3.of("1", baseTime + 3000, 3));        // 3秒

        // 2. 延迟数据（属于第一个窗口）
        inputData.add(Tuple3.of("1", baseTime + 2500, 6));        // 2.5秒
        inputData.add(Tuple3.of("1", baseTime + 3500, 7));        // 3.5秒

        // 3. 第二个窗口的数据（5-10秒）
        inputData.add(Tuple3.of("1", baseTime + 8000, 4));        // 8秒
        inputData.add(Tuple3.of("1", baseTime + 9000, 5));        // 9秒

        // 数据流有三个字段：（key, 时间戳, 数值）
        DataStream<Tuple3<String, Long, Integer>> input = env
                .fromData(inputData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f1)
                );

        DataStream<Tuple4<String, String, Integer, String>> allowedLatenessStream = input
                .keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .allowedLateness(Duration.ofSeconds(5))
                .process(new AllowedLatenessFunction());

        allowedLatenessStream.print();

        env.execute("late elements");
    }

    /**
     * ProcessWindowFunction接收的泛型参数分别为：[输入类型、输出类型、Key、Window]
     */
    public static class AllowedLatenessFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, Integer, String>, String, TimeWindow> {

        private transient ValueState<Boolean> isWindowClosed;

        @Override
        public void open(OpenContext parameters) throws Exception {
            isWindowClosed = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("isWindowClosed", Types.BOOLEAN)
            );
        }

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple3<String, Long, Integer>> elements,
                            Collector<Tuple4<String, String, Integer, String>> out) throws Exception {

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long currentWatermark = context.currentWatermark();

            // 如果窗口已关闭，不再输出
            if (isWindowClosed.value() != null && isWindowClosed.value()) {
                return;
            }

            // 标记窗口内的每个元素
            for (Tuple3<String, Long, Integer> element : elements) {
                // 如果元素的时间戳小于当前水印，视为迟到数据
                if (element.f1 > currentWatermark) {
                    out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), element.f2, "late"));
                } else {
                    out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), element.f2, "processed"));
                }
            }

            // 输出窗口结束标记，并标记该窗口已结束
            if (isWindowClosed.value() == null || !isWindowClosed.value()) {
                out.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), -1, "window_end"));
                isWindowClosed.update(true);
            }
        }
    }
}
