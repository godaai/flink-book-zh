package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TransactionWriteSinkExample {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 访问 http://localhost:8082 可以看到Flink Web UI
        conf.setInteger(RestOptions.PORT, 8082);
        // 创建本地执行环境，并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        // 每隔5秒进行一次Checkpoint
        env.getCheckpointConfig().setCheckpointInterval(5 * 1000);

        DataStream<Tuple2<String, Integer>> countStream = env.addSource(new CheckpointedSourceExample.CheckpointedSource());
        // 每隔一定时间模拟一次失败
        DataStream<Tuple2<String, Integer>> result = countStream.map(new CheckpointedSourceExample.FailingMapper(20));

        // 类Unix系统的临时文件夹在/tmp下
        // Windows用户需要修改这个目录
        String preCommitPath = "/tmp/flink-sink-precommit";
        String commitedPath = "/tmp/flink-sink-commited";

        if (!Files.exists(Paths.get(preCommitPath))) {
            Files.createDirectory(Paths.get(preCommitPath));
        }
        if (!Files.exists(Paths.get(commitedPath))) {
            Files.createDirectory(Paths.get(commitedPath));
        }
        // 使用Exactly-Once语义的Sink，执行本程序时可以查看相应的输出目录，查看数据
        result.addSink(new TwoPhaseFileSink(preCommitPath, commitedPath));
        // 数据打印到屏幕上，无Exactly-Once保障，有数据重发现象
        result.print();
        env.execute("two file sink");
    }

    public static class TwoPhaseFileSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, String, Void> {
        // 缓存
        private BufferedWriter transactionWriter;
        private String preCommitPath;
        private String commitedPath;

        public TwoPhaseFileSink(String preCommitPath, String commitedPath) {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
            this.preCommitPath = preCommitPath;
            this.commitedPath = commitedPath;
        }

        @Override
        public void invoke(String transaction, Tuple2<String, Integer> in, Context context) throws Exception {
            transactionWriter.write(in.f0 + " " + in.f1 + "\n");
        }

        @Override
        public String beginTransaction() throws Exception {
            String time = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            int subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String fileName = time + "-" + subTaskIdx;
            Path preCommitFilePath = Paths.get(preCommitPath + "/" + fileName);
            // 创建一个存储本次事务的文件
            Files.createFile(preCommitFilePath);
            transactionWriter = Files.newBufferedWriter(preCommitFilePath);
            System.out.println("Transaction File: " + preCommitFilePath);

            return fileName;
        }

        @Override
        public void preCommit(String transaction) throws Exception {
            // 将当前数据由内存写入磁盘
            transactionWriter.flush();
            transactionWriter.close();
        }

        @Override
        public void commit(String transaction) {
            Path preCommitFilePath = Paths.get(preCommitPath + "/" + transaction);
            if (Files.exists(preCommitFilePath)) {
                Path commitedFilePath = Paths.get(commitedPath + "/" + transaction);
                try {
                    Files.move(preCommitFilePath, commitedFilePath);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }

        @Override
        public void abort(String transaction) {
            Path preCommitFilePath = Paths.get(preCommitPath + "/" + transaction);

            // 如果中途遇到中断，将文件删除
            if (Files.exists(preCommitFilePath)) {
                try {
                    Files.delete(preCommitFilePath);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }
    }
}
