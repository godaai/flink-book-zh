package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> socketSource1 = env.socketTextStream("localhost", 9000);
        DataStream<String> socketSource2 = env.socketTextStream("localhost", 9001);

        // 数据流有三个字段：（key, 时间戳, 数值）
        DataStream<Tuple3<String, Long, Integer>> input1 = socketSource1.map(
                line -> {
                    String[] arr = line.split(" ");
                    String id = arr[0];
                    long ts = Long.parseLong(arr[1]);
                    int i = Integer.parseInt(arr[2]);
                    return Tuple3.of(id, ts, i);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((event, timestamp) -> event.f1));

        DataStream<Tuple3<String, Long, Integer>> input2 = socketSource2.map(
                line -> {
                    String[] arr = line.split(" ");
                    String id = arr[0];
                    long ts = Long.parseLong(arr[1]);
                    int i = Integer.parseInt(arr[2]);
                    return Tuple3.of(id, ts, i);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((event, timestamp) -> event.f1));

        DataStream<String> intervalJoinResult = input1.keyBy(i -> i.f0)
                .intervalJoin(input2.keyBy(i -> i.f0))
                .between(Time.milliseconds(-5), Time.milliseconds(10))
                .process(new MyProcessFunction());

        intervalJoinResult.print();

        env.execute("interval join function");
    }

    public static class MyProcessFunction extends ProcessJoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String> {
        @Override
        public void processElement(Tuple3<String, Long, Integer> input1,
                                   Tuple3<String, Long, Integer> input2,
                                   Context context,
                                   Collector<String> out) {
            out.collect("input 1: " + input1.toString() + ", input 2: " + input2.toString());
        }
    }
}
