package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class UserBehaviorFromFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 使用当前版本推荐的FileSource API
        String filePath = "src/main/resources/taobao/UserBehavior-test.csv";
        
        // 配置FileSource为连续监控模式，模拟流式输入
        FileSource<UserBehavior> fileSource = FileSource
                .forRecordStreamFormat(new UserBehaviorReaderFormat(), new Path(filePath))
                .monitorContinuously(Duration.ofMillis(500)) // 每500毫秒检查一次文件变化
                .build();

        // 使用更积极的水位线生成策略
        DataStream<UserBehavior> userBehaviorDataStream = env
                .fromSource(fileSource, WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000), "UserBehaviorSource");

        // 使用新版Schema API定义表结构
        tEnv.createTemporaryView(
                "user_behavior", 
                userBehaviorDataStream,
                org.apache.flink.table.api.Schema.newBuilder()
                        .column("userId", DataTypes.BIGINT())
                        .column("itemId", DataTypes.BIGINT())
                        .column("categoryId", DataTypes.INT())
                        .column("behavior", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp` * 1000, 3)")
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build()
        );
        
        // 窗口聚合查询
        Table tumbleGroupByUserId = tEnv.sqlQuery("" +
                "SELECT userId AS user_id, " +
                "COUNT(*) AS cnt, " +
                "TUMBLE_END(ts, INTERVAL '5' SECOND) AS endTs " +
                "FROM user_behavior " +
                "GROUP BY userId, TUMBLE(ts, INTERVAL '5' SECOND)");
        
        // 使用新版API转换为流
        DataStream<Row> result = tEnv.toChangelogStream(tumbleGroupByUserId);
        
        // 设置较低的并行度便于调试
        env.setParallelism(1);

        env.execute("table api");
    }
}

// 输出
// Raw Data: > (1,665,1001,pv,1512057600)
// Raw Data: > (2,266,1000,pv,1512057603)
// Raw Data: > (2,266,777,pv,1512057604)
// Raw Data: > (2,266,666,buy,1512057606)
// Raw Data: > (1,665,555,buy,1512057606)