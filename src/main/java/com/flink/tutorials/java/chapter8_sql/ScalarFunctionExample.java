package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.chapter8_sql.function.IsInFourRing;
import com.flink.tutorials.java.chapter8_sql.function.TimeDiff;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Long, Double, Double, Timestamp>> geoList = new ArrayList<>();
        geoList.add(Tuple4.of(1L, 116.2775, 39.91132, Timestamp.valueOf("2020-03-06 00:00:00")));
        geoList.add(Tuple4.of(2L, 116.44095, 39.88319, Timestamp.valueOf("2020-03-06 00:00:01")));
        geoList.add(Tuple4.of(3L, 116.25965, 39.90478, Timestamp.valueOf("2020-03-06 00:00:02")));
        geoList.add(Tuple4.of(4L, 116.27054, 39.87869, Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Long, Double, Double, Timestamp>> geoStream = env
                .fromCollection(geoList)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Double, Double, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table geoTable = tEnv.fromDataStream(geoStream, "id, long, alt, ts.rowtime, proc.proctime");

        tEnv.createTemporaryView("geo", geoTable);

        tEnv.registerFunction("IsInFourRing", new IsInFourRing());
        tEnv.registerFunction("TimeDiff", new TimeDiff());

        Table inFourRingTab = tEnv.sqlQuery("SELECT id FROM geo WHERE IsInFourRing(long, alt)");
        DataStream<Row> infourRingResult = tEnv.toAppendStream(inFourRingTab, Row.class);
        // 如需查看打印结果，可将注释打开
        // infourRingResult.print();

        Table timeDiffTable = tEnv.sqlQuery("SELECT id, TimeDiff(ts, proc) FROM geo");
        DataStream<Row> timeDiffResult = tEnv.toAppendStream(timeDiffTable, Row.class);
        // 如需查看打印结果，可将注释打开
        // timeDiffResult.print();

        List<Tuple4<Long, String, String, Timestamp>> geoStrList = new ArrayList<>();
        geoStrList.add(Tuple4.of(1L, "116.2775", "39.91132", Timestamp.valueOf("2020-03-06 00:00:00")));
        geoStrList.add(Tuple4.of(2L, "116.44095", "39.88319", Timestamp.valueOf("2020-03-06 00:00:01")));
        geoStrList.add(Tuple4.of(3L, "116.25965", "39.90478", Timestamp.valueOf("2020-03-06 00:00:02")));
        geoStrList.add(Tuple4.of(4L, "116.27054", "39.87869", Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Long, String, String, Timestamp>> geoStrStream = env
                .fromCollection(geoStrList)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, String, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table geoStrTable = tEnv.fromDataStream(geoStrStream, "id, long, alt, ts.rowtime, proc.proctime");

        tEnv.createTemporaryView("geo_str", geoStrTable);
        Table inFourRingStrTab = tEnv.sqlQuery("SELECT id FROM geo_str WHERE IsInFourRing(long, alt)");
        DataStream<Row> infourRingStrResult = tEnv.toAppendStream(inFourRingStrTab, Row.class);
        infourRingStrResult.print();

        env.execute("table api");
    }
}
