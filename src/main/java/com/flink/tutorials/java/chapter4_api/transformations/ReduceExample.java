package com.flink.tutorials.java.chapter4_api.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This class shows different way to implement a `reduce()` transformation:
 *    * lambda function
 *    * implement ReduceFunction class
 * */

public class ReduceExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Score> dataStream = senv.fromElements(
                Score.of("Li", "English", 90), Score.of("Wang", "English", 88),
                Score.of("Li", "Math", 85), Score.of("Wang", "Math", 92),
                Score.of("Liu", "Math", 91), Score.of("Liu", "English", 87));

        // implement ReduceFunction class
        // 实现ReduceFunction
        DataStream<Score> sumReduceFunctionStream = dataStream
                .keyBy(item -> item.name)
                .reduce(new MyReduceFunction());

        sumReduceFunctionStream.print();

        // lambda function
        // 使用 Lambda 表达式
        DataStream<Score> sumLambdaStream = dataStream
                .keyBy(item -> item.name)
                .reduce((s1, s2) -> Score.of(s1.name, "Sum", s1.score + s2.score));
        sumLambdaStream.print();

        senv.execute("basic reduce transformation");
    }

    public static class Score {
        public String name;
        public String course;
        public int score;

        public Score(){}

        public Score(String name, String course, int score) {
            this.name = name;
            this.course = course;
            this.score = score;
        }

        public static Score of(String name, String course, int score) {
            return new Score(name, course, score);
        }

        @Override
        public String toString() {
            return "(" + this.name + ", " + this.course + ", " + Integer.toString(this.score) + ")";
        }
    }

    public static class MyReduceFunction implements ReduceFunction<Score> {
        @Override
        public Score reduce(Score s1, Score s2) {
            return Score.of(s1.name, "Sum", s1.score + s2.score);
        }
    }
}
