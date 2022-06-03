package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class TextFileExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 文件路径
        String filePath = TextFileExample.class.getClassLoader().getResource("stock/stock-test.csv").getPath();

        // 文件为纯文本格式
        TextInputFormat textInputFormat = new TextInputFormat(new org.apache.flink.core.fs.Path(filePath));

        // 每隔100毫秒检测一遍
//        DataStream<String> inputStream = env.readFile(
//                textInputFormat,
//                filePath,
//                FileProcessingMode.PROCESS_CONTINUOUSLY,
//                100);

        // 只读一次
        DataStream<String> readOnceStream = env.readFile(
                textInputFormat,
                filePath,
                FileProcessingMode.PROCESS_ONCE,
                0);

        StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(new Path(filePath + "output-test"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .build();
        readOnceStream.addSink(fileSink);

        env.execute("read write file from path");
    }
}
