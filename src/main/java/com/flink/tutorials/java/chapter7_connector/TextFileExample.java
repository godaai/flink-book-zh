package com.flink.tutorials.java.chapter7_connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.legacy.StreamingFileSink;

import java.io.File;

public class TextFileExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 文件路径
        String filePath = TextFileExample.class.getClassLoader().getResource("stock/stock-test.csv").getPath();
        Path path = new Path(filePath);
        
        // 打印输入文件路径
        System.out.println("输入文件路径: " + filePath);
        
        // 输出文件路径 - 在输入文件所在目录下创建output-test目录
        File inputFile = new File(filePath);
        String outputPath = new File(inputFile.getParent(), "output-test").getPath();
        System.out.println("输出文件路径: " + outputPath);

        // 使用新的 FileSource API 创建文件源
        // 只读一次（BOUNDED模式）
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), path)
                .build();

        // 使用新的Source API读取文件
        DataStream<String> readOnceStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "TextFileSource"
        );

        // 添加打印操作查看数据
        readOnceStream.print("读取的数据");

        // 也可以创建连续读取的文件源（CONTINUOUS模式）
        /*
        FileSource<String> continuousFileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), path)
                .monitorContinuously(Duration.ofMillis(100))
                .build();
                
        DataStream<String> continuousStream = env.fromSource(
                continuousFileSource,
                WatermarkStrategy.noWatermarks(),
                "ContinuousTextFileSource"
        );
        */

        StreamingFileSink<String> fileSink = StreamingFileSink
                .forRowFormat(new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .build();
        readOnceStream.addSink(fileSink);

        System.out.println("开始执行作业...");
        env.execute("read write file from path");
        System.out.println("作业执行完成！");
    }
}
