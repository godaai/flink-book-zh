package com.flink.tutorials.java.chapter5_time;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CoGroupExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用EventTime时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> socketSource1 = env.socketTextStream("localhost", 9000);
        DataStream<String> socketSource2 = env.socketTextStream("localhost", 9001);

        DataStream<Tuple2<String, Integer>> input1 = socketSource1.map(
                line -> {
                    String[] arr = line.split(" ");
                    String id = arr[0];
                    int t = Integer.parseInt(arr[1]);
                    return Tuple2.of(id, t);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        DataStream<Tuple2<String, Integer>> input2 = socketSource2.map(
                line -> {
                    String[] arr = line.split(" ");
                    String id = arr[0];
                    int t = Integer.parseInt(arr[1]);
                    return Tuple2.of(id, t);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<String> coGroupResult = input1.coGroup(input2)
                .where(i1 -> i1.f0)
                .equalTo(i2 -> i2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new MyCoGroupFunction());

        coGroupResult.print();

        env.execute("window cogroup function");
    }

    public static class MyCoGroupFunction implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
        @Override
        public void coGroup(Iterable<Tuple2<String, Integer>> input1, Iterable<Tuple2<String, Integer>> input2, Collector<String> out) {
            input1.forEach(element -> System.out.println("input1 :" + element.f1));
            input2.forEach(element -> System.out.println("input2 :" + element.f1));
        }
    }
}
