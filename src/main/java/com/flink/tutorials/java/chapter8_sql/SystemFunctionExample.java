package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class SystemFunctionExample {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        List<Tuple4<Long, Long, String, Timestamp>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:02")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", Timestamp.valueOf("2020-03-06 00:00:12")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", Timestamp.valueOf("2020-03-06 00:00:13")));

        DataStream<Tuple4<Long, Long, String, Timestamp>> userBehaviorStream = env
                .fromCollection(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table userBehaviorTable = tEnv.fromDataStream(userBehaviorStream, "user_id, item_id, behavior,ts.rowtime");

        tEnv.createTemporaryView("user_behavior", userBehaviorTable);

        Table arrayTab = tEnv.sqlQuery("SELECT CARDINALITY(arr) FROM (SELECT ARRAY[user_id, item_id, 1000] AS arr FROM user_behavior)");
        DataStream<Row> result = tEnv.toAppendStream(arrayTab, Row.class);
        result.print();

        env.execute("table api");
    }
}
