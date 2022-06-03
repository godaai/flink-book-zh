package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class UserBehaviorUVPerMin {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
//                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'user_behavior',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")");

        Table groupByUserId = tEnv.sqlQuery("SELECT user_id, COUNT(behavior) AS behavior_cnt FROM user_behavior GROUP BY user_id");

        String explanation = groupByUserId.explain();
        System.out.println(explanation);

        Table simpleUv = tEnv.sqlQuery("" +
                "SELECT " +
                "   user_id, " +
                "   " +
                "   COUNT(*) OVER w AS uv " +
                "FROM user_behavior " +
                "WINDOW w AS (" +
                "ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
        DataStream<Tuple2<Boolean, Row>> simpleUvResult = tEnv.toRetractStream(simpleUv, Row.class);

        Table cumulativeUv = tEnv.sqlQuery("" +
                "SELECT time_str, MAX(uv) FROM (" +
                "SELECT " +
                "   MAX(SUBSTR(DATE_FORMAT(ts, 'HH:mm'), 1, 4) || '0') OVER w AS time_str, " +
                "   COUNT(DISTINCT user_id) OVER w AS uv " +
                "FROM user_behavior " +
                "WINDOW w AS (" +
                "ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))" +
                "GROUP BY time_str");
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(cumulativeUv, Row.class);
        result.print();

        env.execute("table api");
    }
}
