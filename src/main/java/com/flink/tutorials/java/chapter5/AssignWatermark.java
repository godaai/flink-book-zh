package com.flink.tutorials.java.chapter5;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AssignWatermark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 每5000毫秒生成一个Watermark
        env.getConfig().setAutoWatermarkInterval(5000L);
        DataStream<String> socketSource = env.socketTextStream("localhost", 9000);
        DataStream<Tuple2<String, Long>> input = socketSource.map(
                line -> {
                    String[] arr = line.split(" ");
                    String id = arr[0];
                    long time = Long.parseLong(arr[1]);
                    return Tuple2.of(id, time);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 第二个字段是时间戳
        DataStream<Tuple2<String, Long>> watermark = input.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator((context -> new MyPeriodicGenerator()))
                .withTimestampAssigner((event, recordTimestamp) -> event.f1));

        watermark.print();

        env.execute("periodic and punctuated watermark");
    }

    // 定期生成Watermark
    // 数据流元素 Tuple2<String, Long> 共两个字段
    // 第一个字段为数据本身
    // 第二个字段是时间戳
    public static class MyPeriodicGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

        private final long maxOutOfOrderness = 60 * 1000; // 1分钟
        private long currentMaxTimestamp;                 // 已抽取的Timestamp最大值

        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
            // 更新currentMaxTimestamp为当前遇到的最大值
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Watermark比currentMaxTimestamp最大值慢1分钟
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));
        }

    }

    // 逐个检查数据流中的元素，根据元素中的特殊字段，判断是否要生成Watermark
    // 数据流元素 Tuple3<String, Long, Boolean> 共三个字段
    // 第一个字段为数据本身
    // 第二个字段是时间戳
    // 第三个字段判断是否为Watermark的标记
    public static class MyPunctuatedGenerator implements WatermarkGenerator<Tuple3<String, Long, Boolean>> {

        @Override
        public void onEvent(Tuple3<String, Long, Boolean> event, long eventTimestamp, WatermarkOutput output) {
            if (event.f2) {
                output.emitWatermark(new Watermark(event.f1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 这里不需要做任何事情，因为我们在 onEvent() 方法中生成了Watermark
        }

    }
}
