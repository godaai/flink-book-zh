package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.chapter8_sql.function.WeightedAvg;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class AggFuncExample {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Integer, Long, Long, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 100l, 1l, Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(1, 200l, 2l, Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 300l, 3l, Timestamp.valueOf("2020-03-06 00:00:13")));

        DataStream<Tuple4<Integer, Long, Long, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Integer, Long, Long, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table table = tEnv.fromDataStream(stream, "id, v, w, ts.rowtime");

        tEnv.createTemporaryView("input_table", table);

        tEnv.registerFunction("WeightAvg", new WeightedAvg());

        Table agg = tEnv.sqlQuery("SELECT id, WeightAvg(v, w) FROM input_table GROUP BY id");
        DataStream<Tuple2<Boolean, Row>> aggResult = tEnv.toRetractStream(agg, Row.class);
        aggResult.print();

        env.execute("table api");
    }
}
