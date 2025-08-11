package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TransactionWriteSinkExample {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 访问 http://localhost:8082 可以看到Flink Web UI
        conf.set(RestOptions.PORT, 8082);
        // 创建本地执行环境，并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        // 每隔5秒进行一次Checkpoint
        env.getCheckpointConfig().setCheckpointInterval(5 * 1000);

        // 使用新 Source API
        DataStream<Tuple2<String, Integer>> countStream = env.fromSource(
                new CheckpointedCustomSource(),
                WatermarkStrategy.noWatermarks(),
                "CheckpointedCustomSource"
        );
        
        // 每隔一定时间模拟一次失败
        DataStream<Tuple2<String, Integer>> result = countStream.map(new CheckpointedSourceExample.FailingMapper(20));

        // 类Unix系统的临时文件夹在/tmp下
        // Windows用户可能需要修改这个目录
        String outputPath = System.getProperty("java.io.tmpdir") + "/flink-sink-output";
        System.out.println("输出目录: " + outputPath);
        
        // 确保输出目录存在
        java.nio.file.Path outputDir = Paths.get(outputPath);
        if (!Files.exists(outputDir)) {
            Files.createDirectory(outputDir);
        }

        // 使用FileSink替换旧的TwoPhaseCommitSinkFunction
        // 通过OnCheckpointRollingPolicy确保exactly-once语义
        final FileSink<Tuple2<String, Integer>> sink = FileSink
                .forRowFormat(new Path(outputPath), 
                        (Tuple2<String, Integer> element, OutputStream stream) -> {
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(stream));
                    writer.println(element.f0 + " " + element.f1);
                    writer.flush();
                })
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        // 应用sink
        result.sinkTo(sink);
        
        // 数据打印到屏幕上，无Exactly-Once保障，有数据重发现象
        result.print();
        env.execute("file sink with exactly-once");
    }

    // 自定义 Source（新 API）
    static class CheckpointedCustomSource implements Source<Tuple2<String, Integer>, CheckpointedCustomSplit, Integer> {
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED; // 有限数据源
        }

        @Override
        public SourceReader<Tuple2<String, Integer>, CheckpointedCustomSplit> createReader(SourceReaderContext context) {
            return new CheckpointedSourceReader();
        }

        @Override
        public SplitEnumerator<CheckpointedCustomSplit, Integer> createEnumerator(SplitEnumeratorContext<CheckpointedCustomSplit> context) {
            List<CheckpointedCustomSplit> splits = new ArrayList<>();
            splits.add(new CheckpointedCustomSplit(0));
            return new CheckpointedSplitEnumerator(splits, context);
        }

        @Override
        public SplitEnumerator<CheckpointedCustomSplit, Integer> restoreEnumerator(
                SplitEnumeratorContext<CheckpointedCustomSplit> context, Integer checkpoint) {
            List<CheckpointedCustomSplit> splits = new ArrayList<>();
            splits.add(new CheckpointedCustomSplit(checkpoint));
            return new CheckpointedSplitEnumerator(splits, context);
        }

        @Override
        public SimpleVersionedSerializer<CheckpointedCustomSplit> getSplitSerializer() {
            return new CheckpointedSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
            return new CheckpointSerializer();
        }
    }

    // 自定义 Split（数据分片）
    static class CheckpointedCustomSplit implements SourceSplit {
        private int offset;

        public CheckpointedCustomSplit(int offset) {
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
            return "checkpointed-split";
        }
    }

    // 自定义 SourceReader
    static class CheckpointedSourceReader implements SourceReader<Tuple2<String, Integer>, CheckpointedCustomSplit> {
        private CheckpointedCustomSplit currentSplit;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();
        private boolean isRunning = true;

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availabilityFuture;
        }

        @Override
        public void start() {
            // 不再在此处调度，等待 Split 分配
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Tuple2<String, Integer>> output) throws Exception {
            if (currentSplit == null) {
                return InputStatus.NOTHING_AVAILABLE; // 等待 Split 分配
            }
            if (currentSplit.getOffset() >= 1000 || !isRunning) {
                return InputStatus.END_OF_INPUT;
            }
            
            // 生成数据并递增 Offset
            output.collect(new Tuple2<>(String.valueOf(currentSplit.getOffset()), 1));
            currentSplit.setOffset(currentSplit.getOffset() + 1);
            
            // 重置可用性并调度下一次生成
            availabilityFuture = new CompletableFuture<>();
            scheduleNextWakeup(100); // 与原来的休眠时间保持一致

            return InputStatus.MORE_AVAILABLE;
        }

        private void scheduleNextWakeup(long delayMillis) {
            scheduler.schedule(() -> {
                availabilityFuture.complete(null); // 触发唤醒
            }, delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public List<CheckpointedCustomSplit> snapshotState(long checkpointId) {
            return Collections.singletonList(currentSplit);
        }

        @Override
        public void addSplits(List<CheckpointedCustomSplit> splits) {
            currentSplit = splits.get(0);
            // 触发首次数据生成
            availabilityFuture.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {
            isRunning = false;
            scheduler.shutdown();
        }
    }

    // 自定义 SplitEnumerator（分片分配器）
    static class CheckpointedSplitEnumerator implements SplitEnumerator<CheckpointedCustomSplit, Integer> {
        private final List<CheckpointedCustomSplit> splits;
        private final SplitEnumeratorContext<CheckpointedCustomSplit> context;

        public CheckpointedSplitEnumerator(List<CheckpointedCustomSplit> splits, SplitEnumeratorContext<CheckpointedCustomSplit> context) {
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
        public void addSplitsBack(List<CheckpointedCustomSplit> splits, int subtaskId) {
            this.splits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {
            // 分配分片给指定子任务
            if (!splits.isEmpty()) {
                // 分配 Split 给当前子任务
                CheckpointedCustomSplit split = splits.remove(0);
                context.assignSplits(new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(split))
                ));
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public Integer snapshotState(long checkpointId) {
            // 如果还有未分配的split，返回其offset
            if (!splits.isEmpty()) {
                return splits.get(0).getOffset();
            }
            return 0; // 默认值
        }

        @Override
        public void close() {}
    }

    // 序列化器实现
    static class CheckpointedSplitSerializer implements SimpleVersionedSerializer<CheckpointedCustomSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CheckpointedCustomSplit split) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(bos)) {
                dos.writeInt(split.getOffset());
                return bos.toByteArray();
            }
        }

        @Override
        public CheckpointedCustomSplit deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                 DataInputStream dis = new DataInputStream(bis)) {
                return new CheckpointedCustomSplit(dis.readInt());
            }
        }
    }

    static class CheckpointSerializer implements SimpleVersionedSerializer<Integer> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Integer offset) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(bos)) {
                dos.writeInt(offset);
                return bos.toByteArray();
            }
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) throws IOException {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                 DataInputStream dis = new DataInputStream(bis)) {
                return dis.readInt();
            }
        }
    }
}
