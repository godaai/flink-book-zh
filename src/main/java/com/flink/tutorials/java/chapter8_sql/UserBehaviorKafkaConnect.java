package com.flink.tutorials.java.chapter8_sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class UserBehaviorKafkaConnect {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv
            // 使用connect()函数连接外部系统 connect()部分地方有bug，未来将被废弃
            .connect(
                new Kafka()
                .version("universal")     // 必填，合法的参数有"0.8", "0.9", "0.10", "0.11"或"universal"
                .topic("user_behavior")   // 必填，Topic名
                .startFromEarliest()        // 首次消费时数据读取的位置
                .property("zookeeper.connect", "localhost:2181")  // Kafka连接参数
                .property("bootstrap.servers", "localhost:9092")
            )
            // 序列化方式 可以是JSON、Avro等
            .withFormat(new Json())
            // 数据的Schema
            .withSchema(
                new Schema()
                    .field("user_id", DataTypes.BIGINT())
                    .field("item_id", DataTypes.BIGINT())
                    .field("category_id", DataTypes.BIGINT())
                    .field("behavior", DataTypes.STRING())
                    .field("ts", DataTypes.TIMESTAMP(3))
                    .rowtime(new Rowtime().timestampsFromField("ts").watermarksPeriodicAscending())
            )
            // 临时表的表名，后续可以在SQL语句中使用这个表名
            .createTemporaryTable("user_behavior");

        Table all = tEnv.sqlQuery("SELECT * FROM user_behavior");
        DataStream<Tuple2<Boolean, Row>> allResult = tEnv.toRetractStream(all, Row.class);
        allResult.print();

//        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT \n" +
//                "\tuser_id, \n" +
//                "\tCOUNT(behavior) AS behavior_cnt, \n" +
//                "\tTUMBLE_END(ts, INTERVAL '10' SECOND) AS end_ts \n" +
//                "FROM user_behavior\n" +
//                "GROUP BY user_id, TUMBLE(ts, INTERVAL '10' SECOND)");
//        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(tumbleGroupByUserId, Row.class);
//        result.print();

        env.execute("table api");
    }
}
