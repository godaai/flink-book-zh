package com.flink.tutorials.java.chapter8_sql;

import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class UserBehaviorFromDataStream {
    private static final Logger LOG = LoggerFactory.getLogger(UserBehaviorFromDataStream.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String filePath = "src/main/resources/taobao/UserBehavior-20171201.csv";
        // 修改为连续监控模式，每500毫秒检查一次文件变化
        FileSource<UserBehavior> fileSource = FileSource
                .forRecordStreamFormat(new UserBehaviorReaderFormat(), new Path(filePath))
                .monitorContinuously(Duration.ofMillis(500))
                .build();

        // 使用更积极的水位线生成策略
        DataStream<UserBehavior> userBehaviorDataStream = env
                .fromSource(fileSource, WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000), "UserBehaviorSource");

        // 使用 Schema Builder 定义表结构
        Schema schema = Schema.newBuilder()
                .column("userId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("categoryId", DataTypes.INT())
                .column("behavior", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`timestamp` * 1000, 3)") // 转换为毫秒
                .watermark("ts", "ts - INTERVAL '5' SECOND") // 允许5秒延迟
                .build();
        
        tEnv.createTemporaryView("user_behavior", userBehaviorDataStream, schema);

        // 简化SQL查询，使用更小的时间窗口并限制数据量
        Table tumbleGroupByUserId = tEnv.sqlQuery("SELECT " +
                "userId AS user_id, " +
                "COUNT(*) AS behavior_cnt, " +
                "TUMBLE_END(ts, INTERVAL '5' SECOND) AS end_ts " +
                "FROM user_behavior " +
                "WHERE userId % 1000 = 0 " +  // 只处理部分数据
                "GROUP BY userId, TUMBLE(ts, INTERVAL '5' SECOND)");
        
        DataStream<Row> result = tEnv.toChangelogStream(tumbleGroupByUserId);
        
        // 设置较低的并行度可能有助于调试
        env.setParallelism(1);

        env.execute("table api");
    }
}
