package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimpleSourceExample {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, conf);

        DataStream<Tuple2<String, Integer>> countStream = env.fromSource(
                new CustomSource(),
                WatermarkStrategy.noWatermarks(),
                "CustomSource"
        );

        System.out.println("parallelism: " + env.getParallelism());
        countStream.print();
        env.execute("source");
    }

    // 自定义 Source（新 API）
    static class CustomSource implements Source<Tuple2<String, Integer>, CustomSplit, Void> {
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED; // 有限数据源
        }

        @Override
        public SourceReader<Tuple2<String, Integer>, CustomSplit> createReader(SourceReaderContext context) {
            return new CustomSourceReader();
        }

        @Override
        public SplitEnumerator<CustomSplit, Void> createEnumerator(SplitEnumeratorContext<CustomSplit> context) {
            // 传递 context 到 CustomSplitEnumerator 构造函数
            List<CustomSplit> splits = new ArrayList<>();
            for (int i = 0; i < 2; i++) { // 生成与并行度匹配的 Split 数量
                splits.add(new CustomSplit(0));
            }
            return new CustomSplitEnumerator(splits, context);
        }

        @Override
        public SplitEnumerator<CustomSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<CustomSplit> context, Void checkpoint) {
            return createEnumerator(context);
        }

        @Override
        public SimpleVersionedSerializer<CustomSplit> getSplitSerializer() {
            return new SplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            return new CheckpointSerializer();
        }
    }

    // 自定义 Split（数据分片）
    static class CustomSplit implements SourceSplit {
        private int offset;

        public CustomSplit(int offset) {
            this.offset = offset;
        }

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        @Override
        public String splitId() {
            return "single-split"; // 仅使用单个分片
        }
    }

    // 自定义 SourceReader
    static class CustomSourceReader implements SourceReader<Tuple2<String, Integer>, CustomSplit> {
        private CustomSplit currentSplit;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availabilityFuture;
        }

        @Override
        public void start(){
            // 不再在此处调度，等待 Split 分配
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Tuple2<String, Integer>> output) throws Exception {
            if (currentSplit == null) {
                return InputStatus.NOTHING_AVAILABLE; // 等待 Split 分配
            }
            if (currentSplit.getOffset() >= 1000) {
                return InputStatus.END_OF_INPUT;
            }
            // 生成数据并递增 Offset
            output.collect(new Tuple2<>(String.valueOf(currentSplit.getOffset()), currentSplit.getOffset()));
            currentSplit.setOffset(currentSplit.getOffset() + 1);
            // 重置可用性并调度下一次生成
            availabilityFuture = new CompletableFuture<>();
            scheduleNextWakeup(500);

            return InputStatus.MORE_AVAILABLE;
        }

        private void scheduleNextWakeup(long delayMillis) {
            scheduler.schedule(() -> {
                availabilityFuture.complete(null); // 触发唤醒
            }, delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public List<CustomSplit> snapshotState(long checkpointId) {
            return Collections.singletonList(currentSplit);
        }

        @Override
        public void addSplits(List<CustomSplit> splits) {
            currentSplit = splits.get(0);
            // 触发首次数据生成
            availabilityFuture.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {
            scheduler.shutdown();
        }
    }

    // 自定义 SplitEnumerator（分片分配器）
    static class CustomSplitEnumerator implements SplitEnumerator<CustomSplit, Void> {
        private List<CustomSplit> splits;
        private final SplitEnumeratorContext<CustomSplit> context;

        public CustomSplitEnumerator(List<CustomSplit> splits, SplitEnumeratorContext<CustomSplit> context) {
            this.splits = new ArrayList<>(splits);
            this.context = context;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            // 无需动态分配
        }

        @Override
        public void addSplitsBack(List<CustomSplit> splits, int subtaskId) {
            // 无需处理失败重试
        }

        @Override
        public void addReader(int subtaskId) {
            // 分配分片给指定子任务
            if (!splits.isEmpty()) {
                // 分配第一个 Split 给当前子任务
                CustomSplit split = splits.remove(0);
                context.assignSplits(new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(split))
                ));
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {}
    }

    // 序列化器实现
    static class SplitSerializer implements SimpleVersionedSerializer<CustomSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CustomSplit split) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(bos)) {
                dos.writeInt(split.getOffset());
                return bos.toByteArray();
            }
        }

        @Override
        public CustomSplit deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                 DataInputStream dis = new DataInputStream(bis)) {
                return new CustomSplit(dis.readInt());
            }
        }
    }

    static class CheckpointSerializer implements SimpleVersionedSerializer<Void> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Void obj) {
            return new byte[0];
        }

        @Override
        public Void deserialize(int version, byte[] serialized) {
            return null;
        }
    }
}