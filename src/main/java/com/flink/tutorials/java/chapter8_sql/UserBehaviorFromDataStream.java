package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class UserBehaviorFromDataStream {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBehavior> userBehaviorDataStream = env
                .addSource(new UserBehaviorSource("taobao/UserBehavior-20171201.csv"))
                // 在DataStream里设置时间戳和Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        tEnv.createTemporaryView("user_behavior", userBehaviorDataStream, "userId as user_id, itemId as item_id, categoryId as category_id, behavior, ts.rowtime");

        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT " +
                "user_id, " +
                "COUNT(behavior) AS behavior_cnt, " +
                "TUMBLE_END(ts, INTERVAL '10' SECOND) AS end_ts " +
                "FROM user_behavior " +
                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND)");
        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);

//         如果使用ProcessingTime，可以使用下面的代码
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        tEnv.createTemporaryView("user_behavior", userBehaviorDataStream,
//                "userId as user_id, itemId as item_id, categoryId as category_id, behavior, proctime.proctime");
//
//        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT " +
//                "user_id, " +
//                "COUNT(behavior) AS behavior_cnt, " +
//                "TUMBLE_END(proctime, INTERVAL '10' SECOND) AS end_ts " +
//                "FROM user_behavior " +
//                "GROUP BY user_id, TUMBLE(proctime, INTERVAL '10' SECOND)");
//        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);
//
        result.print();

        env.execute("table api");
    }
}
