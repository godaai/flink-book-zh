package com.flink.tutorials.java.chapter4.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class MapExample {

    public static void main(String[] args) throws Exception {

        // 创建 Flink 执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Integer> dataStream = senv.fromElements(1, 2, -3, 0, 5, -9, 8);

        // 使用Lambda表达式
        DataStream<String> lambdaStream = dataStream
                .map(input -> "lambda input : " + input + ", output : " + (input * 2));
        lambdaStream.print();

        DataStream<String> functionDataStream = dataStream.map(new DoubleMapFunction());
        functionDataStream.print();

        // 匿名类
        DataStream<String> anonymousDataStream = dataStream.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer input) throws Exception {
                return "anonymous function input : " + input + ", output : " + (input * 2);
            }
        });
        anonymousDataStream.print();

        senv.execute("basic map transformation");
    }

    // 继承MapFunction
    // 第一个泛型Integer是输入类型，第二个泛型String是输出类型
    public static class DoubleMapFunction implements MapFunction<Integer, String> {
        @Override
        public String map(Integer input) {
            return "function input : " + input + ", output : " + (input * 2);
        }
    }
}
