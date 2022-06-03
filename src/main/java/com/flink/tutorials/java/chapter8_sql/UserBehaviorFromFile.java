package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


public class UserBehaviorFromFile {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        // Flink .rowtime() 定义时间戳的方法有bug，本方法将在未来版本废弃
        Schema schema = new Schema()
                .field("user_id", DataTypes.BIGINT())
                .field("item_id", DataTypes.BIGINT())
                .field("category", DataTypes.BIGINT())
                .field("behavior", DataTypes.STRING())
                .field("ts", DataTypes.TIMESTAMP(3))
                .rowtime(new Rowtime().timestampsFromField("ts").watermarksPeriodicBounded(1000));

        String filePath = UserBehaviorFromFile.class
                .getClassLoader().getResource("taobao/UserBehavior-test.csv")
                .getPath();

        // connect()方法定义数据源未来将被废弃，以后主要使用SQL DDL
        tEnv.connect(new FileSystem().path(filePath))
        .withFormat(new Csv())
        .withSchema(schema)
        .createTemporaryTable("user_behavior");

        Table userBehaviorTable = tEnv.from("user_behavior");
        Table groupByUser = userBehaviorTable.groupBy("user_id").select("user_id, COUNT(behavior) as cnt");

        Table groupByUserId = tEnv.sqlQuery("SELECT user_id, COUNT(behavior) AS cnt FROM user_behavior GROUP BY user_id");

        Table tumbleGroupByUserId = tEnv.sqlQuery("" +
                "SELECT user_id, TUMBLE_END(ts, INTERVAL '5' MINUTE) AS endTs, COUNT(behavior) AS cnt " +
                "FROM user_behavior " +
                "GROUP BY user_id, TUMBLE(ts, INTERVAL '5' MINUTE)");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);
        result.print();


        env.execute("table api");
    }
}
