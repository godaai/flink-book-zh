package com.flink.tutorials.java.api.table;

import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class UserBehaviorFromDataStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("user_id", DataTypes.BIGINT())
                .field("item_id", DataTypes.BIGINT())
                .field("category", DataTypes.BIGINT())
                .field("behavior", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT());

//        Table userBehaviorTable = tEnv.from("user_behavior");
//        Table groupByUser = userBehaviorTable.groupBy("user_id").select("user_id, COUNT(behavior) as cnt");

        DataStream<UserBehavior> userBehaviorDataStream = env.addSource(new UserBehaviorSource("taobao/UserBehavior-test.csv"));
        tEnv.createTemporaryView("user_behavior_file", userBehaviorDataStream);

        Table groupByUser = tEnv.sqlQuery("SELECT userId FROM user_behavior_file");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(groupByUser, Row.class);
        result.print();

        env.execute("table api");
    }
}
