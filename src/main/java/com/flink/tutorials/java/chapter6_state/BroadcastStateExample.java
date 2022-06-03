package com.flink.tutorials.java.chapter6_state;

import com.flink.tutorials.java.utils.taobao.BehaviorPattern;
import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 主数据流
        DataStream<UserBehavior> userBehaviorStream = env.addSource(new UserBehaviorSource("taobao/UserBehavior-20171201.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000)
                );

        // BehaviorPattern数据流
        DataStream<BehaviorPattern> patternStream = env.fromElements(new BehaviorPattern("pv", "buy"));

        // Broadcast State只能使用 Key->Value 结构，基于MapStateDescriptor
        MapStateDescriptor<Void, BehaviorPattern> broadcastStateDescriptor = new MapStateDescriptor<>("behaviorPattern", Types.VOID, Types.POJO(BehaviorPattern.class));
        BroadcastStream<BehaviorPattern> broadcastStream = patternStream.broadcast(broadcastStateDescriptor);

        // 生成一个KeyedStream
        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorStream.keyBy(user -> user.userId);

        // 在KeyedStream上进行connect和process
        DataStream<Tuple2<Long, BehaviorPattern>> matchedStream = keyedStream
                .connect(broadcastStream)
                .process(new BroadcastPatternFunction());

        matchedStream.print();

        env.execute("broadcast taobao example");
    }

    /**
     * 四个泛型分别为：
     * 1. KeyedStream中Key的数据类型
     * 2. 主数据流的数据类型
     * 3. 广播流的数据类型
     * 4. 输出类型
     * */
    public static class BroadcastPatternFunction
            extends KeyedBroadcastProcessFunction<Long, UserBehavior, BehaviorPattern, Tuple2<Long, BehaviorPattern>> {

        // 用户上次行为状态句柄，每个用户存储一个状态
        private ValueState<String> lastBehaviorState;
        // Broadcast State Descriptor
        private MapStateDescriptor<Void, BehaviorPattern> bcPatternDesc;

        @Override
        public void open(Configuration configuration) {
            lastBehaviorState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("lastBehaviorState", Types.STRING));
            bcPatternDesc = new MapStateDescriptor<Void, BehaviorPattern>("behaviorPattern", Types.VOID, Types.POJO(BehaviorPattern.class));
        }

        @Override
        public void processBroadcastElement(BehaviorPattern pattern,
                                            Context context,
                                            Collector<Tuple2<Long, BehaviorPattern>> collector) throws Exception {
            BroadcastState<Void, BehaviorPattern> bcPatternState = context.getBroadcastState(bcPatternDesc);
            // 将新数据更新至Broadcast State，这里使用一个null作为Key
            // 在本场景中所有数据都共享一个Pattern，因此这里伪造了一个Key
            bcPatternState.put(null, pattern);
        }

        @Override
        public void processElement(UserBehavior userBehavior,
                                   ReadOnlyContext context,
                                   Collector<Tuple2<Long, BehaviorPattern>> collector) throws Exception {

            // 获取最新的Broadcast State
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
