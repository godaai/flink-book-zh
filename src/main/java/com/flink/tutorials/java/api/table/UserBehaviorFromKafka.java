package com.flink.tutorials.java.api.table;

import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

public class UserBehaviorFromKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("user_id", DataTypes.BIGINT())
                .field("item_id", DataTypes.BIGINT())
                .field("category", DataTypes.BIGINT())
                .field("behavior", DataTypes.STRING())
                .field("ts", DataTypes.TIMESTAMP(3));

        tEnv.connect(new Kafka()
                .version("universal")
                .topic("user_behavior")
                .startFromEarliest()
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("user_behavior_kafka");

        Table groupByUser = tEnv.sqlQuery("SELECT user_id FROM user_behavior_kafka");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(groupByUser, Row.class);
        result.print();

        env.execute("table api");
    }
}
