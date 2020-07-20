package com.flink.tutorials.java.chapter5;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

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
        DataStream<Tuple2<String, Long>> watermark = input.assignTimestampsAndWatermarks(new MyPeriodicAssigner());
        DataStream boundedOutOfOrder = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        });

        env.execute("periodic and punctuated watermark");
    }

    public static class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        private long bound = 60 * 1000;        // 1分钟
        private long maxTs = Long.MIN_VALUE;   // 已抽取的Timestamp最大值

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            // 更新maxTs为当前遇到的最大值
            maxTs = Math.max(maxTs, element.f1);
            return element.f1;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // Watermark比Timestamp最大值慢1分钟
            Watermark watermark = new Watermark(maxTs - bound);
            return watermark;
        }
    }

    // 第二个字段是时间戳，第三个字段判断是否为Watermark的标记
    public static class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Tuple3<String, Long, Boolean>> {
        @Override
        public long extractTimestamp(Tuple3<String, Long, Boolean> element, long previousElementTimestamp) {
            return element.f1;
        }

        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<String, Long, Boolean> element, long extractedTimestamp) {
            if (element.f2) {
                return new Watermark(extractedTimestamp);
            } else {
                return null;
            }
        }
    }
}
