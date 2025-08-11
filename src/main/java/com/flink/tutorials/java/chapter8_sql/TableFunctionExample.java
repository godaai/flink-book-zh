package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.chapter8_sql.function.TableFunc;
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

public class TableFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple4<Integer, Long, String, Instant>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 1L, "Jack#22", LocalDateTime.parse("2020-03-06T00:00:00").atZone(ZoneId.systemDefault()).toInstant()));
        list.add(Tuple4.of(2, 2L, "John#19", LocalDateTime.parse("2020-03-06T00:00:01").atZone(ZoneId.systemDefault()).toInstant()));
        list.add(Tuple4.of(3, 3L, "nosharp", LocalDateTime.parse("2020-03-06T00:00:03").atZone(ZoneId.systemDefault()).toInstant()));

        DataStream<Tuple4<Integer, Long, String, Instant>> stream = env
                .fromData(list)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Integer, Long, String, Instant>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.toEpochMilli())
                );

        Table table = tEnv.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())            // id
                        .column("f1", DataTypes.BIGINT())         // long
                        .column("f2", DataTypes.STRING())         // str
                        .column("f3", DataTypes.TIMESTAMP_LTZ(3)) // ts
                        .watermark("f3", "f3 - INTERVAL '0' SECOND")
                        .build());

        tEnv.createTemporaryView("input_table", table);

        tEnv.createTemporarySystemFunction("Func", TableFunc.class);

        // input_table与LATERAL TABLE(Func(str))进行JOIN
        Table tableFunc = tEnv.sqlQuery("SELECT f0 as id, s FROM input_table, LATERAL TABLE(Func(f2)) AS T(s)");
        DataStream<Row> tableFuncResult = tEnv.toChangelogStream(tableFunc);
        // 如需查看打印结果，可将注释打开
         tableFuncResult.print();

        // input_table与LATERAL TABLE(Func(str))进行LEFT JOIN
        Table joinTableFunc = tEnv.sqlQuery("SELECT f0 as id, s FROM input_table LEFT JOIN LATERAL TABLE(Func(f2)) AS T(s) ON TRUE");
        DataStream<Row> joinTableFuncResult = tEnv.toChangelogStream(joinTableFunc);
//        joinTableFuncResult.print();

        env.execute("table api");
    }
}

// 输出
// join 普通JOIN中没有输出 +I[3, null]，因为他不输出没有匹配到的记录
// +I[1, Jack]
// +I[1, 22]
// +I[2, 19]
// +I[2, John]

// left join
// +I[1, 22] 对于 John#22，因为分割为两个，所以 ID为 1的记录值是 22 和 John
// +I[2, John] 一样 John#19
// +I[1, Jack]
// +I[3, null] 而因为 nosharp 没有分割，所以 ID为 3的记录值是 null
// +I[2, 19]