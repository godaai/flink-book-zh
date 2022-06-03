package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class CheckpointedSourceExample {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 访问 http://localhost:8082 可以看到Flink Web UI
        conf.setInteger(RestOptions.PORT, 8082);
        // 创建本地执行环境，并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        // 每隔2秒进行一次Checkpoint
        env.getCheckpointConfig().setCheckpointInterval(2 * 1000);

        DataStream<Tuple2<String, Integer>> countStream = env.addSource(new CheckpointedSource());
        // 每隔一定时间模拟一次失败
        DataStream<Tuple2<String, Integer>> result = countStream.map(new FailingMapper(20));
        result.print();
        env.execute("checkpointed source");
    }


    public static class CheckpointedSource
            extends RichSourceFunction<Tuple2<String, Integer>>
            implements CheckpointedFunction {

        private int offset;
        private boolean isRunning = true;
        private ListState<Integer> offsetState;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(100);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new Tuple2<>("" + offset, 1));
                    offset++;
                }
                if (offset == 1000) {
                    isRunning = false;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext snapshotContext) throws Exception {
            // 清除上次状态
            offsetState.clear();
            // 将最新的offset添加到状态中
            offsetState.add(offset);
        }

        @Override
        public void initializeState(FunctionInitializationContext initializationContext) throws Exception {
            // 初始化offsetState
            ListStateDescriptor<Integer> desc = new ListStateDescriptor<Integer>("offset", Types.INT);
            offsetState = initializationContext.getOperatorStateStore().getListState(desc);

            Iterable<Integer> iter = offsetState.get();
            if (iter == null || !iter.iterator().hasNext()) {
                // 第一次初始化，从0开始计数
                offset = 0;
            } else {
                // 从状态中恢复offset
                offset = iter.iterator().next();
            }
        }
    }

    public static class FailingMapper
            implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private int count = 0;
        private int failInterval;

        public FailingMapper(int failInterval) {
            this.failInterval = failInterval;
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) {
            count += 1;
            if (count > failInterval) {
                throw new RuntimeException("job fail! show how flink checkpoint and recovery");
            }
            return in;
        }
    }
}
