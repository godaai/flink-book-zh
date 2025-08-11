package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.chapter8_sql.function.IsInFourRing;
import com.flink.tutorials.java.chapter8_sql.function.TimeDiff;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.math.BigDecimal;

public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册自定义函数 - 使用实例注册
        tEnv.createTemporarySystemFunction("IsInFourRing", new IsInFourRing());
        tEnv.createTemporarySystemFunction("TimeDiff", new TimeDiff());

        // 使用Row直接创建数据表
        // 创建测试数据表 - 数值类型经纬度
        tEnv.createTemporaryView(
                "geo",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("longitude", DataTypes.DECIMAL(10, 6)),
                                DataTypes.FIELD("latitude", DataTypes.DECIMAL(10, 6)),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3))
                        ),
                        Row.of(1L, new BigDecimal("116.2775"), new BigDecimal("39.91132"), LocalDateTime.parse("2020-03-06T00:00:00")),
                        Row.of(2L, new BigDecimal("116.44095"), new BigDecimal("39.88319"), LocalDateTime.parse("2020-03-06T00:00:01")),
                        Row.of(3L, new BigDecimal("116.25965"), new BigDecimal("39.90478"), LocalDateTime.parse("2020-03-06T00:00:02")),
                        Row.of(4L, new BigDecimal("116.27054"), new BigDecimal("39.87869"), LocalDateTime.parse("2020-03-06T00:00:03"))
                )
        );

        // 创建测试数据表 - 字符串类型经纬度
        tEnv.createTemporaryView(
                "geo_str",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("longitude_str", DataTypes.STRING()),
                                DataTypes.FIELD("latitude_str", DataTypes.STRING()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3))
                        ),
                        Row.of(1L, "116.2775", "39.91132", LocalDateTime.parse("2020-03-06T00:00:00")),
                        Row.of(2L, "116.44095", "39.88319", LocalDateTime.parse("2020-03-06T00:00:01")),
                        Row.of(3L, "116.25965", "39.90478", LocalDateTime.parse("2020-03-06T00:00:02")),
                        Row.of(4L, "116.27054", "39.87869", LocalDateTime.parse("2020-03-06T00:00:03"))
                )
        );

        // 添加处理时间列
        tEnv.executeSql("CREATE VIEW geo_with_proc AS SELECT *, PROCTIME() AS proc FROM geo");
        tEnv.executeSql("CREATE VIEW geo_str_with_proc AS SELECT *, PROCTIME() AS proc FROM geo_str");

        // 查询数据表结构，查看实际的列名
        System.out.println("== 数值表结构 ==");
        tEnv.executeSql("DESCRIBE geo_with_proc").print();
        
        System.out.println("== 字符串表结构 ==");
        tEnv.executeSql("DESCRIBE geo_str_with_proc").print();

        // 查询1：检查点是否在四环内
        Table inFourRingTab = tEnv.sqlQuery(
                "SELECT id, longitude, latitude " +
                "FROM geo_with_proc WHERE IsInFourRing(longitude, latitude)"
        );
        
        // 查询2：处理String类型的经纬度，注意使用正确的列名
        Table inFourRingStrTab = tEnv.sqlQuery(
                "SELECT id, longitude_str, latitude_str, " +
                "IsInFourRing(longitude_str, latitude_str) AS is_in_four_ring " +
                "FROM geo_str_with_proc"
        );

        // 查询3：计算时间差
        Table timeDiffTab = tEnv.sqlQuery(
                "SELECT id, TimeDiff(ts, proc) AS time_diff FROM geo_with_proc"
        );

        System.out.println("=== 查询1 结果: 四环内的点 ===");
        DataStream<Row> result1 = tEnv.toChangelogStream(inFourRingTab);
        result1.print();

        System.out.println("=== 查询2 结果: 字符串转换后四环内的点 ===");
        DataStream<Row> result2 = tEnv.toChangelogStream(inFourRingStrTab);
        result2.print();

        System.out.println("=== 查询3 结果: 时间差 ===");
        DataStream<Row> result3 = tEnv.toChangelogStream(timeDiffTab);
        result3.print();

        // 执行
        env.execute("Scalar Function Example");
    }
}

// 结果
// === 查询1 结果: 四环内的点 ===
// +I[2, 116.440950, 39.883190]
// +I[1, 116.277500, 39.911320]
// +I[4, 116.270540, 39.878690]

// === 查询2 结果: 字符串转换后四环内的点 ===

// === 查询3 结果: 时间差 ===
// +I[1, 158783447562]
// +I[3, 158783445568]
// +I[4, 158783444568]
// +I[2, 158783446566]