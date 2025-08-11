package com.flink.tutorials.java.chapter6_state;

import com.flink.tutorials.java.utils.taobao.BehaviorPattern;
import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorReaderFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 主数据流
        String filePath = ClassLoader.getSystemResource("taobao/UserBehavior-20171201.csv")
                .getPath();
        FileSource<UserBehavior> source = FileSource
                .forRecordStreamFormat(new UserBehaviorReaderFormat(), new Path(filePath))
                .build();

        DataStream<UserBehavior> userBehaviorStream = env.fromSource(source,
                WatermarkStrategy
                        .<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000),
                "BehaviorSource"
        );

        // BehaviorPattern 数据流
        DataStream<BehaviorPattern> patternStream = env.fromData(new BehaviorPattern("pv", "buy"));

        // Broadcast State 只能使用 Key->Value 结构，基于 MapStateDescriptor
        MapStateDescriptor<Void, BehaviorPattern> broadcastStateDescriptor = new MapStateDescriptor<>("behaviorPattern", Types.VOID, Types.POJO(BehaviorPattern.class));
        BroadcastStream<BehaviorPattern> broadcastStream = patternStream.broadcast(broadcastStateDescriptor);

        // 生成一个 KeyedStream
        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorStream.keyBy(user -> user.userId);

        // 在 KeyedStream 上进行 connect 和 process
        DataStream<Tuple2<Long, BehaviorPattern>> matchedStream = keyedStream
                .connect(broadcastStream)
                .process(new BroadcastPatternFunction());

        matchedStream.print();

        env.execute("broadcast taobao example");
    }

    /**
     * 四个泛型分别为：
     * 1. KeyedStream 中 Key 的数据类型
     * 2. 主数据流的数据类型
     * 3. 广播流的数据类型
     * 4. 输出类型
     */
    public static class BroadcastPatternFunction
            extends KeyedBroadcastProcessFunction<Long, UserBehavior, BehaviorPattern, Tuple2<Long, BehaviorPattern>> {

        // 用户上次行为状态句柄，每个用户存储一个状态
        private ValueState<String> lastBehaviorState;
        // Broadcast State Descriptor
        private MapStateDescriptor<Void, BehaviorPattern> bcPatternDesc;

        @Override
        public void open(OpenContext openContext) {
            lastBehaviorState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastBehaviorState", Types.STRING));
            bcPatternDesc = new MapStateDescriptor<>("behaviorPattern", Types.VOID, Types.POJO(BehaviorPattern.class));
        }

        @Override
        public void processBroadcastElement(BehaviorPattern pattern,
                                            Context context,
                                            Collector<Tuple2<Long, BehaviorPattern>> collector) throws Exception {
            BroadcastState<Void, BehaviorPattern> bcPatternState = context.getBroadcastState(bcPatternDesc);
            // 将新数据更新至 Broadcast State，这里使用一个 null 作为 Key
            // 在本场景中所有数据都共享一个 Pattern，因此这里伪造了一个 Key
            bcPatternState.put(null, pattern);
        }

        @Override
        public void processElement(UserBehavior userBehavior,
                                   ReadOnlyContext context,
                                   Collector<Tuple2<Long, BehaviorPattern>> collector) throws Exception {

            // 获取最新的 Broadcast State
            BehaviorPattern pattern = context.getBroadcastState(bcPatternDesc).get(null);
            String lastBehavior = lastBehaviorState.value();
            if (pattern != null && lastBehavior != null) {
                // 用户之前有过行为，检查是否符合给定的模式
                if (pattern.firstBehavior.equals(lastBehavior) &&
                        pattern.secondBehavior.equals(userBehavior.behavior)) {
                    // 当前用户行为符合模式
                    collector.collect(Tuple2.of(userBehavior.userId, pattern));
                }
            }
            lastBehaviorState.update(userBehavior.behavior);
        }
    }
}
