package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class InsertExample {

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

        // 使用Schema Builder来定义表结构，包含时间属性
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

        // 在Flink 2.0中更新表创建语句
        tEnv.executeSql("CREATE TABLE behavior_cnt (\n" +
                "    user_id BIGINT,\n" +
                "    cnt BIGINT" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +  // 使用print连接器替代filesystem，避免文件系统权限问题
                ")");

        tEnv.executeSql("INSERT INTO behavior_cnt SELECT f0 as user_id, COUNT(f2) AS cnt FROM user_behavior GROUP BY f0, TUMBLE(f3, INTERVAL '10' SECOND)");

        env.execute("table api");
    }
}

// 输出
// +I[1, 2] 插入user_id为1，共两条记录（pv）第一个10s
// +I[2, 1] 插入user_id为2，共一条记录（pv）第一个10s
// +I[2, 2] 插入user_id为2，共两条记录（cart 和 buy）第二个10s
