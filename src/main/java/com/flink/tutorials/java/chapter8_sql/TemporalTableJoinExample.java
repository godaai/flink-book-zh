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

public class TemporalTableJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        List<Tuple4<Long, Long, String, Instant>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", LocalDateTime.parse("2020-03-06T00:00:02").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", LocalDateTime.parse("2020-03-06T00:00:03").atZone(ZoneId.systemDefault()).toInstant()));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", LocalDateTime.parse("2020-03-06T00:01:04").atZone(ZoneId.systemDefault()).toInstant()));

        List<Tuple3<Long, Long, Instant>> itemData = new ArrayList<>();
        itemData.add(Tuple3.of(1000L, 299L, LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        itemData.add(Tuple3.of(1001L, 199L, LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        itemData.add(Tuple3.of(1000L, 310L, LocalDateTime.parse("2020-03-06T00:00:15").atZone(ZoneId.systemDefault()).toInstant()));
        itemData.add(Tuple3.of(1001L, 189L, LocalDateTime.parse("2020-03-06T00:00:15").atZone(ZoneId.systemDefault()).toInstant()));

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

        DataStream<Tuple3<Long, Long, Instant>> itemStream = env
                .fromData(itemData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f2.toEpochMilli())
                );
        
        // 使用Schema Builder定义商品表结构，并添加主键，确保主键字段为NOT NULL
        Table itemTable = tEnv.fromDataStream(
                itemStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT().notNull())  // item_id - 添加notNull()确保主键列非空
                        .column("f1", DataTypes.BIGINT())            // price
                        .column("f2", DataTypes.TIMESTAMP_LTZ(3))    // versionTs
                        .watermark("f2", "f2 - INTERVAL '0' SECOND")
                        .primaryKey("f0")                            // 添加主键，用于时态表连接
                        .build());

        // 在Flink 2.0中，使用FOR SYSTEM_TIME AS OF来实现时态表连接
        tEnv.createTemporaryView("item", itemTable);

        // 使用FOR SYSTEM_TIME AS OF语法进行时态表连接
        String sqlQuery = "SELECT \n" +
                "   user_behavior.f1 as item_id," +
                "   item.f1 as price,\n" +
                "   user_behavior.f3 as ts\n" +
                "FROM " +
                "   user_behavior JOIN item FOR SYSTEM_TIME AS OF user_behavior.f3\n" +
                "ON user_behavior.f1 = item.f0\n" +
                "WHERE user_behavior.f2 = 'buy'";

        Table joinResult = tEnv.sqlQuery(sqlQuery);
        DataStream<Row> result = tEnv.toChangelogStream(joinResult);
        result.print();
        System.out.println(tEnv.getConfig().getLocalTimeZone());

        env.execute("table api");
    }
}

// 输出
// Asia/Shanghai (这是 Flink 执行环境的时区)
// +I[1001, 189, 2020-03-05T16:01:04Z] 
// item_id为1001的商品在用户"buy"操作时的价格确实是189元
