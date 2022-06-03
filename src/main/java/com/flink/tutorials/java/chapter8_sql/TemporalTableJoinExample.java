package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class TemporalTableJoinExample {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Long, Long, String, Timestamp>> userBehaviorData = new ArrayList<>();
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "pv", Timestamp.valueOf("2020-03-06 00:00:00")));
        userBehaviorData.add(Tuple4.of(1L, 1000L, "pv", Timestamp.valueOf("2020-03-06 00:00:02")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "cart", Timestamp.valueOf("2020-03-06 00:00:03")));
        userBehaviorData.add(Tuple4.of(2L, 1001L, "buy", Timestamp.valueOf("2020-03-06 00:01:04")));

        List<Tuple3<Long, Long, Timestamp>> itemData = new ArrayList<>();

        itemData.add(Tuple3.of(1000L, 299L, Timestamp.valueOf("2020-03-06 00:00:00")));
        itemData.add(Tuple3.of(1001L, 199L, Timestamp.valueOf("2020-03-06 00:00:00")));
        itemData.add(Tuple3.of(1000L, 310L, Timestamp.valueOf("2020-03-06 00:00:15")));
        itemData.add(Tuple3.of(1001L, 189L, Timestamp.valueOf("2020-03-06 00:00:15")));

        DataStream<Tuple4<Long, Long, String, Timestamp>> userBehaviorStream = env
                .fromCollection(userBehaviorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table userBehaviorTable = tEnv.fromDataStream(userBehaviorStream, "user_id, item_id, behavior,ts.rowtime");
        tEnv.createTemporaryView("user_behavior", userBehaviorTable);

        DataStream<Tuple3<Long, Long, Timestamp>> itemStream = env
                .fromCollection(itemData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<Long, Long, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f2.getTime())
                );
        Table itemTable = tEnv.fromDataStream(itemStream, "item_id, price, versionTs.rowtime");

        // 注册 Temporal Table Function
        tEnv.registerFunction(
                "item",
                itemTable.createTemporalTableFunction("versionTs", "item_id"));

        String sqlQuery = "SELECT \n" +
                "   user_behavior.item_id," +
                "   latest_item.price,\n" +
                "   user_behavior.ts\n" +
                "FROM " +
                "   user_behavior, LATERAL TABLE(item(user_behavior.ts)) AS latest_item\n" +
                "WHERE user_behavior.item_id = latest_item.item_id" +
                "   AND user_behavior.behavior = 'buy'";

        Table joinResult = tEnv.sqlQuery(sqlQuery);
        DataStream<Row> result = tEnv.toAppendStream(joinResult, Row.class);
        result.print();
        System.out.println(tEnv.getConfig().getLocalTimeZone());

        env.execute("table api");
    }
}
