package com.flink.tutorials.java.chapter5_time;

import com.flink.tutorials.java.utils.stock.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;

public class KeyCoProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读入股票数据流
        String StockFilePath = ClassLoader.getSystemResource("stock/stock-tick-20200108.csv")
                .getPath();
        FileSource<StockPrice> stockSource = FileSource
                .forRecordStreamFormat(new StockReaderFormat(), new Path(StockFilePath))
                .build();
        DataStream<StockPrice> stockStream = env
                .fromSource(stockSource, WatermarkStrategy.noWatermarks(), "StockSource");

        // 读入媒体评价数据流
        String mediaFilePath = new File("").getAbsolutePath() + "/media-data.csv";
        // 首先生成测试数据
        MediaDataGenerator.generateMediaData(mediaFilePath, 100);
        FileSource<Media> mediaSource = FileSource.forRecordStreamFormat(new MediaReaderFormat(), new Path(mediaFilePath)).build();

        DataStream<Media> mediaStream = env
                .fromSource(mediaSource,
                        WatermarkStrategy
                                .<Media>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts),
                        "MediaSource");

        DataStream<StockPrice> joinStream = stockStream.connect(mediaStream)
                .keyBy(stock -> stock.symbol, media -> media.symbol)
                // 调用process()函数
                .process(new JoinStockMediaProcessFunction());

        joinStream.print();

        env.execute("coprocess function");
    }

    /**
     * 四个泛型：Key，第一个流类型，第二个流类型，输出。
     */
    public static class JoinStockMediaProcessFunction extends KeyedCoProcessFunction<String, StockPrice, Media, StockPrice> {
        // mediaState
        private ValueState<String> mediaState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            // 从RuntimeContext中获取状态
            mediaState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("mediaStatusState", Types.STRING));
        }

        @Override
        public void processElement1(StockPrice stock, Context context, Collector<StockPrice> collector) throws Exception {
            String mediaStatus = mediaState.value();
            if (null != mediaStatus) {
                stock.mediaStatus = mediaStatus;
                collector.collect(stock);
            }
        }

        @Override
        public void processElement2(Media media, Context context, Collector<StockPrice> collector) throws Exception {
            // 第二个流更新mediaState
            mediaState.update(media.status);
        }
    }
}
