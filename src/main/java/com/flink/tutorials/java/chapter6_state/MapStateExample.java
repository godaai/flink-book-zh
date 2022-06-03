package com.flink.tutorials.java.chapter6_state;

import com.flink.tutorials.java.utils.taobao.UserBehavior;
import com.flink.tutorials.java.utils.taobao.UserBehaviorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 如果使用Checkpoint，可以开启下面三行，Checkpoint将写入HDFS
//        env.enableCheckpointing(2000L);
//        StateBackend stateBackend = new RocksDBStateBackend("hdfs:///flink-ckp");
//        env.setStateBackend(stateBackend);

        DataStream<UserBehavior> userBehaviorStream = env.addSource(new UserBehaviorSource("taobao/UserBehavior-20171201.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.timestamp * 1000)
                );

        // 生成一个KeyedStream
        KeyedStream<UserBehavior, Long> keyedStream =  userBehaviorStream.keyBy(user -> user.userId);

        // 在KeyedStream上进行flatMap()
        DataStream<Tuple3<Long, String, Integer>> behaviorCountStream= keyedStream.flatMap(new MapStateFunction());

        behaviorCountStream.print();

        env.execute("taobao map state example");
    }

    public static class MapStateFunction extends RichFlatMapFunction<UserBehavior, Tuple3<Long, String, Integer>> {

        // 指向MapState的句柄
        private MapState<String, Integer> behaviorMapState;

        @Override
        public void open(Configuration configuration) {
            // 创建StateDescriptor
            MapStateDescriptor<String, Integer> behaviorMapStateDescriptor = new MapStateDescriptor<String, Integer>("behaviorMap", Types.STRING, Types.INT);
            // 通过StateDescriptor获取运行时上下文中的状态
            behaviorMapState = getRuntimeContext().getMapState(behaviorMapStateDescriptor);
        }

        @Override
        public void flatMap(UserBehavior input, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
            int behaviorCnt = 1;
            // behavior有可能为pv、cart、fav、buy等
            // 判断状态中是否有该behavior
            if (behaviorMapState.contains(input.behavior)) {
                behaviorCnt = behaviorMapState.get(input.behavior) + 1;
            }
            // 更新状态
            behaviorMapState.put(input.behavior, behaviorCnt);
            out.collect(Tuple3.of(input.userId, input.behavior, behaviorCnt));
        }
    }
}
