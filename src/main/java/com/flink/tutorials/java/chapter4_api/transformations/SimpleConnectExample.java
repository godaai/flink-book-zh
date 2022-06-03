package com.flink.tutorials.java.chapter4_api.transformations;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class SimpleConnectExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> intStream  = senv.fromElements(1, 0, 9, 2, 3, 6);
        DataStream<String> stringStream  = senv.fromElements("LOW", "HIGH", "LOW", "LOW");

        ConnectedStreams<Integer, String> connectedStream = intStream.connect(stringStream);

        DataStream<String> mapResult = connectedStream.map(new MyCoMapFunction());
        mapResult.print();

        senv.execute("connect");
    }

    // CoMapFunction三个泛型分别对应第一个流的输入、第二个流的输入，map()之后的输出
    public static class MyCoMapFunction implements CoMapFunction<Integer, String, String> {

        @Override
        public String map1(Integer input1) {
            return input1.toString();
        }

        @Override
        public String map2(String input2) {
            return input2;
        }
    }
}
