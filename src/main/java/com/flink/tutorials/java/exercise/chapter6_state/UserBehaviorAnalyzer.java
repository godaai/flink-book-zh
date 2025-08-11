package com.flink.tutorials.java.exercise.chapter6_state;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorReaderFormat;

public class UserBehaviorAnalyzer {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 为了简化示例，设置并行度为1

        // 配置 Checkpoint
        env.enableCheckpointing(60000); // 每60秒做一次checkpoint

        // 使用FileSource读取CSV文件
        String filePath = ClassLoader.getSystemResource("taobao/UserBehavior-20171201.csv")
                .getPath();
        FileSource<UserBehavior> fileSource = FileSource
                .forRecordStreamFormat(new UserBehaviorReaderFormat(), new Path(filePath))
                .build();

        // 从文件源读取数据流
        DataStream<UserBehavior> userBehaviorStream = env.fromSource(
                fileSource,
                WatermarkStrategy
                        .<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000),
                "UserBehaviorSource");

        // 按用户ID分组
        DataStream<Tuple2<Long, Long>> resultStream = userBehaviorStream
                .keyBy(behavior -> behavior.userId)
                .process(new UserBehaviorProcessFunction());

        // 打印结果
        resultStream.print();

        env.execute("User Behavior Time Analysis");
    }

    // 正确的KeyedProcessFunction重写
    public static class UserBehaviorProcessFunction
            extends KeyedProcessFunction<Long, UserBehavior, Tuple2<Long, Long>> {

        private ValueState<Long> firstActionTimeState;
        private ValueState<Long> buyTimeState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 初始化状态
            ValueStateDescriptor<Long> firstActionStateDescriptor =
                    new ValueStateDescriptor<>("firstActionTime", Types.LONG);
            firstActionTimeState = getRuntimeContext().getState(firstActionStateDescriptor);

            ValueStateDescriptor<Long> buyStateDescriptor =
                    new ValueStateDescriptor<>("buyTime", Types.LONG);
            buyTimeState = getRuntimeContext().getState(buyStateDescriptor);
        }

        @Override
        public void processElement(UserBehavior value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
            // 获取当前用户的状态
            Long firstActionTime = firstActionTimeState.value();
            Long buyTime = buyTimeState.value();

            // 如果是购买行为
            if ("buy".equals(value.behavior)) {
                // 如果之前没有记录购买时间，记录当前购买时间
                if (buyTime == null) {
                    buyTimeState.update(value.timestamp * 1000); // 转换为毫秒
                }
                // 如果已经记录了第一次行为时间，计算时间差并输出
                if (firstActionTime != null) {
                    long timeDiff = value.timestamp * 1000 - firstActionTime; // 转换为毫秒
                    out.collect(new Tuple2<>(value.userId, timeDiff));
                }
            } else {
                // 如果不是购买行为，记录第一次行为时间
                if (firstActionTime == null) {
                    firstActionTimeState.update(value.timestamp * 1000); // 转换为毫秒
                }
            }
        }
    }
}
