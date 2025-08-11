package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.chapter8_sql.function.WeightedAvg;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class AggFuncExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 使用Instant替代java.sql.Timestamp
        List<Tuple4<Integer, Long, Long, Instant>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 100L, 1L, LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        list.add(Tuple4.of(1, 200L, 2L, LocalDateTime.parse("2020-03-06T00:00:01").atZone(ZoneId.systemDefault()).toInstant()));
        list.add(Tuple4.of(3, 300L, 3L, LocalDateTime.parse("2020-03-06T00:00:13").atZone(ZoneId.systemDefault()).toInstant()));

        DataStream<Tuple4<Integer, Long, Long, Instant>> stream = env
                .fromData(list)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Integer, Long, Long, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.toEpochMilli())
                );

        // 使用Schema Builder来定义表结构，使用Tuple的默认字段名并确保类型匹配
        Table table = tEnv.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())         // id
                        .column("f1", DataTypes.BIGINT().notNull())  // v (NOT NULL)
                        .column("f2", DataTypes.BIGINT().notNull())  // w (NOT NULL)
                        .column("f3", DataTypes.TIMESTAMP_LTZ(3)) // ts
                        .watermark("f3", "f3 - INTERVAL '0' SECOND")
                        .build());

        tEnv.createTemporaryView("input_table", table);

        tEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);

        // 确保SQL中的函数调用参数类型与函数定义匹配
        // WeightedAvg需要两个BIGINT参数
        Table agg = tEnv.sqlQuery("SELECT f0 as id, WeightedAvg(f1, f2) as weighted_avg FROM input_table GROUP BY f0");
        
        // 直接使用toChangelogStream
        DataStream<Row> aggResult = tEnv.toChangelogStream(agg);
        aggResult.print();

        env.execute("table api");
    }
}

// 输出
// +I[3, 300.0]  插入 id=3的均值为300.0 
// +I[1, 100.0]  插入 id=1的均值为100.0
// -U[1, 100.0]  删除 id=1
// +U[1, 166.66666666666666] 更新 id=1的均值为166.66666666666666
