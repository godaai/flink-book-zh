package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AssignWatermark {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 访问 http://localhost:8082 可以看到Flink Web UI
        conf.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 每隔一定时间生成一个Watermark
        env.getConfig().setAutoWatermarkInterval(1000L);
        DataStream<String> socketSource = env.socketTextStream("localhost", 9000);
        DataStream<Tuple3<String, Long, Long>> input = socketSource.map(
                line -> {
                    String[] arr = line.split(" ");
                    String id = arr[0];
                    long time = Long.parseLong(arr[1]);
                    return Tuple3.of(id, 1L, time);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.LONG));

        DataStream<Tuple3<String, Long, Long>> watermark = input.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator((context -> new MyPeriodicGenerator()))
                        .withTimestampAssigner((event, recordTimestamp) -> event.f2)
        );


        DataStream<Tuple3<String, Long, Long>> wordcount = watermark.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1);

        wordcount.print();

        env.execute("periodic and punctuated watermark");
    }

    // 定期生成Watermark
    // 数据流元素 Tuple2<String, Long, Long> 共三个字段
    // 第一个字段为单词
    // 第二个字段为词频
    // 第三个字段是时间戳
    public static class MyPeriodicGenerator implements WatermarkGenerator<Tuple3<String, Long, Long>> {

        private final long maxOutOfOrderness = 5 * 1000; // 5 秒钟
        private long currentMaxTimestamp;                 // 已抽取的Timestamp最大值

        @Override
        public void onEvent(Tuple3<String, Long, Long> event, long eventTimestamp, WatermarkOutput output) {
            // 更新currentMaxTimestamp为当前遇到的最大值
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Watermark比currentMaxTimestamp最大值慢
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
