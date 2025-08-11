package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class RegularJoinExample {

    public static void main(String[] args) throws Exception {

        System.setProperty("user.timezone","GMT+8");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        // 事件时间在Flink 2.0中是默认设置，不需要显式设置

        List<Tuple4<Long, Long, String, Instant>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:02").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", LocalDateTime.parse("2020-03-06T00:00:03").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", LocalDateTime.parse("2020-03-06T00:01:04").atZone(ZoneId.systemDefault()).toInstant()));

        List<Tuple2<Long, Long>> itemData = new ArrayList<>();
        itemData.add(Tuple2.of(1000L, 310L));
        itemData.add(Tuple2.of(1001L, 189L));

        DataStream<Tuple4<Long, Long, String, Instant>> userBehaviorStream = env
                .fromData(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.toEpochMilli())
                );

        // 使用Schema Builder定义表结构
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

        DataStream<Tuple2<Long, Long>> itemStream = env
                .fromData(itemData);
        
        // 使用Schema Builder定义表结构
        Table itemTable = tEnv.fromDataStream(
                itemStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())       // item_id
                        .column("f1", DataTypes.BIGINT())       // price
                        .build());
        tEnv.createTemporaryView("item", itemTable);

        String sqlQuery = "SELECT \n" +
                "   user_behavior.f1 as item_id," +
                "   item.f1 as price \n" +
                "FROM " +
                "   user_behavior, item\n" +
                "WHERE user_behavior.f1 = item.f0" +
                "   AND user_behavior.f2 = 'buy'";

        Table joinResult = tEnv.sqlQuery(sqlQuery);
        
        // 在Flink 2.0中，使用toChangelogStream替代toAppendStream
        DataStream<Row> result = tEnv.toChangelogStream(joinResult);
        result.print();

        env.execute("table api");
    }
}

// 输出
// +I[1001, 189] 插入item_id为1001，价格为189的记录，符合查询条件