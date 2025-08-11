package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class SystemFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple4<Long, Long, String, Instant>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:02").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", LocalDateTime.parse("2020-03-06T00:00:12").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", LocalDateTime.parse("2020-03-06T00:00:13").atZone(ZoneId.systemDefault()).toInstant()));

        DataStream<Tuple4<Long, Long, String, Instant>> userBehaviorStream = env
                .fromData(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.toEpochMilli())
                );

        Table userBehaviorTable = tEnv.fromDataStream(
                userBehaviorStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())       // user_id
                        .column("f1", DataTypes.BIGINT())       // item_id
                        .column("f2", DataTypes.STRING())       // behavior
                        .column("f3", DataTypes.TIMESTAMP_LTZ(3)) // ts
                        .watermark("f3", "f3 - INTERVAL '0' SECOND")
                        .build());

        tEnv.createTemporaryView("user_behavior", userBehaviorTable);

        Table arrayTab = tEnv.sqlQuery("SELECT CARDINALITY(arr) FROM (SELECT ARRAY[f0, f1, 1000] AS arr FROM user_behavior)");
        DataStream<Row> result = tEnv.toChangelogStream(arrayTab);
        result.print();

        env.execute("table api");
    }
}

// 输出
// +I[3] 插入-数组的元素个数是3
// +I[3]
// +I[3]
// +I[3]
// +I[3]