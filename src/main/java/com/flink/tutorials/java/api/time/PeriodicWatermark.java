package com.flink.tutorials.java.api.time;

import com.flink.tutorials.java.api.utils.random.RandomId;
import com.flink.tutorials.java.api.utils.random.RandomSource;
import com.flink.tutorials.java.api.utils.stock.StockPrice;
import com.flink.tutorials.java.api.utils.stock.StockSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class PeriodicWatermark {

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

//        DataStream<> watermark = input.assignTimestampsAndWatermarks(new MyPeriodicAssigner)
//
//        DataStream<> watermark = input.assignTimestampsAndWatermarks(new MyPeriodicAssigner)
//        val boundedOutOfOrder = input.assignTimestampsAndWatermarks(
//                new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.minutes(1)) {
//            override def extractTimestamp(element: (String, Long)): Long = {
//                    element._2
//            }
//        })

        env.execute("periodic and punctuated watermark");

//        val socketSource = env.socketTextStream("localhost", 9000)
//
//        val input = socketSource.map{
//            line => {
//                val arr = line.split(" ")
//                val id = arr(0)
//                val time = arr(1).toLong
//                        (id, time)
//            }
//        }

        env.execute("event time and watermark");
    }

    public static class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

        private long bound = 60 * 1000;
        private long maxTs = Long.MIN_VALUE;

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

}
