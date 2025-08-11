package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class TimeWindowJoinExample {

    public static void main(String[] args) throws Exception {

        System.setProperty("user.timezone","GMT+8");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        List<Tuple4<Long, Long, String, Instant>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:02").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", LocalDateTime.parse("2020-03-06T00:00:03").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", LocalDateTime.parse("2020-03-06T00:00:17").atZone(ZoneId.systemDefault()).toInstant()));

        List<Tuple3<Long, Long, Instant>> chatData = new ArrayList<>();
        chatData.add(Tuple3.of(1L, 1000L, LocalDateTime.parse("2020-03-06T00:00:05").atZone(ZoneId.systemDefault()).toInstant()));
        chatData.add(Tuple3.of(2L, 1001L, LocalDateTime.parse("2020-03-06T00:00:08").atZone(ZoneId.systemDefault()).toInstant()));

        DataStream<Tuple4<Long, Long, String, Instant>> userBehaviorStream = env
                .fromData(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.toEpochMilli())
                );

        // 使用Schema Builder定义用户行为表结构
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

        DataStream<Tuple3<Long, Long, Instant>> chatStream = env
                .fromData(chatData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f2.toEpochMilli())
                );

        // 使用Schema Builder定义聊天表结构
        Table chatTable = tEnv.fromDataStream(
                chatStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())       // buyer_id
                        .column("f1", DataTypes.BIGINT())       // item_id
                        .column("f2", DataTypes.TIMESTAMP_LTZ(3)) // ts
                        .watermark("f2", "f2 - INTERVAL '0' SECOND")
                        .build());
        
        tEnv.createTemporaryView("chat", chatTable);

        String sqlQuery = "SELECT \n" +
                "    user_behavior.f1 as item_id,\n" +
                "    user_behavior.f3 AS buy_ts\n" +
                "FROM chat, user_behavior\n" +
                "WHERE chat.f1 = user_behavior.f1\n" +
                "    AND user_behavior.f2 = 'buy'\n" +
                "    AND user_behavior.f3 BETWEEN chat.f2 AND chat.f2 + INTERVAL '10' SECOND";

        Table joinResult = tEnv.sqlQuery(sqlQuery);
        DataStream<Row> result = tEnv.toChangelogStream(joinResult);
        result.print();
        System.out.println(tEnv.getConfig().getLocalTimeZone());

        env.execute("table api");
    }
}

// 输出 - 查询条件是：购买行为发生在聊天时间后的10秒内
// GMT+08:00
// +I[1001, 2020-03-05T16:00:17Z]
